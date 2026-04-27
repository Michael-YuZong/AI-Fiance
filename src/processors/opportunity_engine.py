"""Unified opportunity discovery and analysis engine."""

from __future__ import annotations

import io
import math
import re
import threading
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import redirect_stderr
from dataclasses import dataclass
from datetime import datetime
from functools import lru_cache
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence
from urllib.parse import urlparse

import numpy as np
import pandas as pd

from src.commands.intel import collect_intel_news_report
from src.collectors import (
    AssetLookupCollector,
    ChinaMarketCollector,
    CommodityCollector,
    EventsCollector,
    FundProfileCollector,
    GlobalFlowCollector,
    IndustryIndexCollector,
    IndexTopicCollector,
    MarketDriversCollector,
    MarketMonitorCollector,
    MarketPulseCollector,
    NewsCollector,
    SocialSentimentCollector,
    ValuationCollector,
)
from src.collectors.base import BaseCollector
from src.processors.context import (
    derive_regime_inputs,
    global_proxy_runtime_enabled,
    load_china_macro_snapshot,
    load_global_proxy_snapshot,
)
from src.processors.factor_meta import FACTOR_REGISTRY, factor_meta_payload
from src.processors.horizon import build_analysis_horizon_profile, horizon_family_code
from src.processors.provenance import build_analysis_provenance
from src.processors.regime import RegimeDetector
from src.processors.signal_confidence import build_signal_confidence
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.output.theme_playbook import build_theme_playbook_context, subject_theme_label, subject_theme_terms
from src.utils.config import detect_asset_type, resolve_project_path
from src.utils.data import load_watchlist, load_yaml
from src.utils.fund_taxonomy import (
    BROAD_TECH_KEYWORDS,
    CARRIER_COMMUNICATION_KEYWORDS,
    CHIP_KEYWORDS,
    COMMUNICATION_KEYWORDS,
    COMMUNICATION_DEVICE_KEYWORDS,
    CXO_KEYWORDS,
    DATA_CENTER_COMMUNICATION_KEYWORDS,
    GAME_MEDIA_KEYWORDS,
    GRID_STORAGE_KEYWORDS,
    HK_INNOVATIVE_DRUG_KEYWORDS,
    INNOVATIVE_DRUG_KEYWORDS,
    MEDIA_KEYWORDS,
    MEDICAL_DEVICE_KEYWORDS,
    POWER_EQUIPMENT_KEYWORDS,
    SATELLITE_COMMUNICATION_KEYWORDS,
    SEMICONDUCTOR_EQUIPMENT_KEYWORDS,
    SEMICONDUCTOR_KEYWORDS,
    SMART_GRID_KEYWORDS,
    build_standard_fund_taxonomy,
    build_theme_taxonomy_profile,
    uses_index_mainline,
)
from src.utils.market import (
    build_snapshot_fallback_history,
    build_intraday_snapshot,
    close_yfinance_runtime_caches,
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
    (("沪深300", "中证a500", "a500", "中证500", "上证50", "上证综指", "上证综合", "上证指数", "宽基"), "宽基", ["宽基", "大盘蓝筹", "内需"]),
    (SMART_GRID_KEYWORDS, "电网", ["特高压", "智能电网", "电网设备"]),
    (GRID_STORAGE_KEYWORDS, "电网", ["储能并网", "新型储能", "电力设备"]),
    (POWER_EQUIPMENT_KEYWORDS, "电网", ["电力设备", "智能电网", "储能并网"]),
    (("电网", "电力", "特高压"), "电网", ["电力需求", "智能电网", "电网设备"]),
    (("黄金", "贵金属"), "黄金", ["黄金", "通胀预期"]),
    (("金融", "银行", "券商", "证券", "保险", "多元金融", "非银"), "金融", ["银行", "券商", "保险"]),
    (
        ("化工", "煤化工", "煤基", "甲醇", "烯烃", "聚乙烯", "聚丙烯", "焦化", "焦炭"),
        "材料",
        ["煤化工", "化工材料", "顺周期"],
    ),
    (HK_INNOVATIVE_DRUG_KEYWORDS, "医药", ["创新药", "港股医药", "FDA"]),
    (INNOVATIVE_DRUG_KEYWORDS, "医药", ["创新药", "医药研发", "BD授权"]),
    (CXO_KEYWORDS, "医药", ["CXO", "CRO/CDMO", "医药外包"]),
    (MEDICAL_DEVICE_KEYWORDS, "医药", ["医疗器械", "设备更新", "老龄化"]),
    (SEMICONDUCTOR_EQUIPMENT_KEYWORDS, "半导体", ["半导体设备", "半导体材料", "国产替代"]),
    (CHIP_KEYWORDS, "半导体", ["芯片", "半导体", "国产替代"]),
    (SEMICONDUCTOR_KEYWORDS, "半导体", ["半导体", "芯片", "国产替代"]),
    (SATELLITE_COMMUNICATION_KEYWORDS, "通信", ["卫星通信", "卫星互联网", "商业航天"]),
    (COMMUNICATION_DEVICE_KEYWORDS, "通信", ["CPO", "光模块", "通信设备"]),
    (DATA_CENTER_COMMUNICATION_KEYWORDS, "通信", ["数据中心", "通信设备", "AI算力"]),
    (CARRIER_COMMUNICATION_KEYWORDS, "通信", ["运营商", "通信服务", "5G/6G"]),
    (COMMUNICATION_KEYWORDS, "通信", ["通信设备", "通信服务", "网络基础设施"]),
    (GAME_MEDIA_KEYWORDS, "传媒", ["游戏", "传媒", "AI应用"]),
    (MEDIA_KEYWORDS, "传媒", ["游戏", "传媒", "AI应用"]),
    (BROAD_TECH_KEYWORDS, "科技", ["AI算力", "软件服务", "成长股估值修复"]),
    (("油", "煤", "能源"), "能源", ["原油", "通胀预期", "能源安全"]),
    (("银行", "红利", "高股息"), "高股息", ["高股息", "防守"]),
    (("医药", "医疗", "创新药", "生物医药", "制药"), "医药", ["医药", "老龄化"]),
    (("农业", "农牧", "农林", "粮食", "粮油", "种业", "种植", "农化", "化肥", "农资", "粮食安全"), "农业", ["粮食安全", "种业", "农化"]),
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
    "电力设备": {3, 4, 5},
    "电网": {3, 4, 5},
    "科技": {6, 7, 8},
    "半导体": {6, 7, 8},
    "通信": {6, 7, 8},
    "传媒": {7, 8, 12},
    "能源": {9, 10, 11},
    "军工": {7, 8, 9},
    "有色": {2, 3, 4},
    "农业": {3, 4, 9, 10},
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

NEGATIVE_THEME_HEADWIND_KEYS = (
    "下修",
    "下调",
    "砍单",
    "订单下滑",
    "需求疲软",
    "需求走弱",
    "库存高企",
    "库存积压",
    "价格战",
    "降价",
    "亏损扩大",
    "不及预期",
    "失速",
    "恶化",
    "裁员",
    "产能过剩",
    "oversupply",
    "weak demand",
    "inventory build",
    "price war",
    "guidance cut",
    "cuts forecast",
    "misses estimates",
    "recall",
)

NEGATIVE_EVENT_LOOKBACK_DAYS = 30
FORWARD_EVENT_LOOKAHEAD_DAYS = 14
DIRECT_COMPANY_NEWS_LOOKBACK_DAYS = 45
CATALYST_FRESH_NEWS_DAYS = 3
ETF_THEME_CATALYST_LOOKBACK_DAYS = 7
HOLDER_TRADE_LOOKBACK_DAYS = 90
CAPITAL_RETURN_LOOKBACK_DAYS = 365
STRUCTURED_EVENT_FULL_SCORE_DAYS = 45
STRUCTURED_EVENT_DECAY_DAYS = 120
SIGNAL_CONFIDENCE_TOP_LIMIT = 8
FUND_ACHIEVEMENT_3M_ALIASES = ("近3月", "近三月", "近3个月", "3个月", "最近3月")

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
    "cninfo",
    "巨潮资讯",
    "上交所",
    "深交所",
    "sse",
    "szse",
    "公司公告",
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
    "投资者关系",
    "活动记录表",
    "调研纪要",
    "路演纪要",
    "业绩说明会",
    "交流纪要",
    "电话会纪要",
    "互动平台",
    "互动易",
    "投资者问答",
    "e互动",
)

IR_INTERACTION_KEYS = (
    "投资者关系",
    "活动记录表",
    "调研纪要",
    "路演纪要",
    "业绩说明会",
    "交流纪要",
    "电话会纪要",
    "互动平台",
    "互动易",
    "投资者问答",
    "e互动",
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
    "半导体": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "通信": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "传媒": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "黄金": {"rate": -1, "usd": -1, "oil": 1, "cny": 1},
    "能源": {"rate": 0, "usd": 1, "oil": 1, "cny": -1},
    "金融": {"rate": 1, "usd": 0, "oil": 0, "cny": 0},
    "高股息": {"rate": 1, "usd": 0, "oil": 0, "cny": 0},
    "医药": {"rate": -1, "usd": 0, "oil": -1, "cny": 1},
    "农业": {"rate": -1, "usd": -1, "oil": 1, "cny": 1},
    "消费": {"rate": -1, "usd": -1, "oil": -1, "cny": 1},
    "军工": {"rate": 0, "usd": 1, "oil": 1, "cny": -1},
    "有色": {"rate": -1, "usd": -1, "oil": 1, "cny": 1},
}

MACRO_LEADING_MAP = {
    "宽基": {"recovery": 1, "credit": 1, "reflation": 0, "defensive": 0},
    "电网": {"recovery": 1, "credit": 1, "reflation": 0, "defensive": 1},
    "科技": {"recovery": 1, "credit": 1, "reflation": -1, "defensive": -1},
    "半导体": {"recovery": 1, "credit": 1, "reflation": -1, "defensive": -1},
    "通信": {"recovery": 1, "credit": 1, "reflation": -1, "defensive": -1},
    "传媒": {"recovery": 1, "credit": 0, "reflation": -1, "defensive": -1},
    "黄金": {"recovery": -1, "credit": -1, "reflation": 1, "defensive": 1},
    "能源": {"recovery": -1, "credit": -1, "reflation": 1, "defensive": -1},
    "金融": {"recovery": 1, "credit": 1, "reflation": 0, "defensive": 0},
    "高股息": {"recovery": -1, "credit": -1, "reflation": 0, "defensive": 1},
    "医药": {"recovery": 0, "credit": 0, "reflation": -1, "defensive": 1},
    "农业": {"recovery": 0, "credit": 1, "reflation": 1, "defensive": 1},
    "消费": {"recovery": 1, "credit": 1, "reflation": -1, "defensive": 0},
    "军工": {"recovery": 1, "credit": 0, "reflation": 1, "defensive": 0},
    "有色": {"recovery": 1, "credit": 1, "reflation": 1, "defensive": -1},
}

NEWS_KEYWORD_ALIASES = {
    "宽基": ["沪深300", "中证A500", "A500", "上证综指", "上证综合指数", "上证指数", "large cap", "blue chip", "宽基", "broad market"],
    "科技": ["科技", "ai", "artificial intelligence", "software", "cloud", "云计算", "互联网", "算力", "人工智能"],
    "半导体": ["半导体", "芯片", "semiconductor", "chip", "chips", "foundry", "fab", "wafer", "存储", "HBM", "chiplet"],
    "通信": ["通信", "telecom", "通信设备", "光模块", "光通信", "cpo", "co-packaged optics", "800g", "1.6t", "交换机", "以太网", "数据中心", "idc", "5g", "6g", "运营商", "电信"],
    "传媒": ["传媒", "media", "游戏", "game", "gaming", "动漫", "影视", "aigc", "ai应用", "版号", "广告"],
    "黄金": ["黄金", "gold", "bullion", "precious metal", "贵金属"],
    "电网": ["电网", "电力", "grid", "power", "utility", "electricity", "特高压", "储能"],
    "能源": ["能源", "oil", "opec", "gas", "原油", "煤炭"],
    "金融": ["金融", "bank", "broker", "brokerage", "insurance", "capital market", "银行", "证券", "券商", "保险", "多元金融"],
    "高股息": ["高股息", "红利", "dividend", "yield", "utility"],
    "医药": ["医药", "医疗", "biotech", "pharma", "drug", "医疗器械"],
    "农业": ["农业", "agriculture", "grain", "粮食", "粮油", "种业", "seed", "fertilizer", "化肥", "农化", "农资", "food security"],
    "消费": ["消费", "零售", "retail", "消费电子", "beer", "food"],
    "军工": ["军工", "国防", "defense", "aerospace", "制裁"],
    "有色": ["有色", "copper", "aluminum", "metal", "铜", "铝", "矿业"],
    "AI算力": ["ai", "artificial intelligence", "gpu", "model", "llm", "算力"],
    "半导体": ["semiconductor", "chip", "foundry", "fab", "wafer", "存储", "芯片", "半导体"],
    "国产替代": ["国产替代", "localisation", "domestic supply", "自主可控"],
    "通胀预期": ["inflation", "通胀", "cpi"],
}

VALUATION_KEYWORD_MAP = {
    "宽基": ["沪深300", "中证A500", "中证500", "上证50", "上证综指", "上证综合指数"],
    "电网": ["电网", "智能电网"],
    "科技": ["科技", "人工智能", "软件", "算力"],
    "半导体": ["半导体", "芯片", "集成电路"],
    "通信": ["通信", "通信设备", "光模块", "运营商", "数据中心"],
    "传媒": ["传媒", "游戏", "动漫", "影视"],
    "黄金": ["黄金"],
    "能源": ["能源", "油气", "煤炭"],
    "金融": ["金融", "银行", "证券", "券商", "保险", "多元金融"],
    "高股息": ["红利", "高股息"],
    "医药": ["医药"],
    "农业": ["农业", "粮食", "种业", "农林牧渔", "农化"],
    "消费": ["消费"],
    "军工": ["军工"],
    "有色": ["有色", "铜", "铝"],
}

THEME_INDEX_KEYWORD_MAP = {
    "光模块": ["光模块", "光通信", "CPO", "通信设备", "通信"],
    "通信设备": ["通信设备", "通信", "光通信", "光模块"],
    "AI算力": ["AI算力", "算力", "人工智能", "服务器", "数据中心"],
    "数据中心": ["数据中心", "服务器", "算力", "IDC"],
    "PCB": ["PCB", "印制电路", "电子元件"],
    "消费电子": ["消费电子"],
    "游戏": ["游戏", "动漫", "传媒", "AIGC"],
    "传媒": ["传媒", "游戏", "影视", "动漫"],
    "创新药": ["创新药", "医药", "生物医药"],
    "商业航天": ["商业航天", "卫星", "航天"],
    "电网设备": ["电网设备", "智能电网", "特高压"],
    "农业": ["农业", "粮食", "种业", "农化", "化肥"],
}

BOARD_MATCH_ALIASES = {
    "宽基": ["沪深300", "中证A500", "中证500", "上证50", "上证综指", "上证综合指数", "宽基"],
    "电网": ["电网", "电力", "电网设备", "智能电网", "特高压", "公用事业"],
    "科技": ["人工智能", "AI", "消费电子", "软件服务", "算力", "互联网", "机器人"],
    "半导体": ["半导体", "芯片", "集成电路", "存储", "先进封装"],
    "通信": ["通信", "通信设备", "光模块", "光通信", "CPO", "运营商", "数据中心", "IDC", "电信"],
    "传媒": ["传媒", "游戏", "动漫", "影视", "AIGC", "广告营销"],
    "黄金": ["黄金", "贵金属"],
    "能源": ["能源", "油气", "石油", "煤炭", "天然气"],
    "高股息": ["红利", "高股息", "电信", "运营商", "公用事业"],
    "医药": ["医药", "医疗", "创新药", "医疗器械"],
    "农业": ["农业", "农林牧渔", "种业", "化肥", "粮食", "饲料"],
    "消费": ["消费", "食品饮料", "家电", "零售", "旅游"],
    "军工": ["军工", "国防军工", "航天航空", "商业航天", "军民融合", "卫星"],
    "有色": ["有色金属", "工业金属", "铜", "铝", "黄金"],
}

GENERIC_CATALYST_PROFILES = {
    "科技": {
        "themes": ["科技", "AI算力", "成长股估值修复"],
        "keywords": ["科技", "ai", "cloud", "software", "算力", "云", "大模型", "互联网"],
        "policy_keywords": ["人工智能", "算力", "软件", "数字经济", "云计算", "科技"],
        "domestic_leaders": ["中际旭创", "工业富联", "寒武纪", "中科曙光", "浪潮信息", "金山办公"],
        "overseas_leaders": ["Microsoft", "Apple", "NVIDIA", "Amazon", "Meta", "Alphabet", "Broadcom", "AMD"],
        "earnings_keywords": ["earnings", "results", "guidance", "capex", "cloud", "AI", "财报", "指引", "资本开支"],
        "event_keywords": ["财报", "指引", "资本开支", "capex", "云", "AI", "产品发布", "模型发布"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 15, "overseas": 20, "news_density": 10, "news_heat": 10, "forward_event": 5},
    },
    "半导体": {
        "themes": ["半导体", "芯片", "先进封装"],
        "keywords": ["半导体", "芯片", "semiconductor", "chip", "foundry", "fab", "HBM", "chiplet", "capex", "先进封装"],
        "policy_keywords": ["集成电路", "算力", "自主可控", "国产替代", "先进封装", "大基金"],
        "domestic_leaders": ["中芯国际", "北方华创", "中微公司", "长电科技", "澜起科技"],
        "overseas_leaders": ["TSMC", "NVIDIA", "ASML", "Micron", "Samsung", "AMD"],
        "earnings_keywords": ["earnings", "results", "guidance", "capex", "财报", "指引", "扩产", "量产"],
        "event_keywords": ["财报", "扩产", "量产", "资本开支", "先进封装", "涨价"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 15, "overseas": 20, "news_density": 10, "news_heat": 10, "forward_event": 5},
    },
    "通信": {
        "themes": ["通信", "CPO", "光模块", "数据中心"],
        "keywords": ["通信", "光模块", "光通信", "cpo", "交换机", "以太网", "800g", "1.6t", "数据中心", "idc", "5g", "6g", "运营商"],
        "policy_keywords": ["算力基础设施", "6G", "5G-A", "万兆光网", "数据中心", "数字经济", "东数西算"],
        "domestic_leaders": ["中际旭创", "新易盛", "天孚通信", "中兴通讯", "烽火通信", "中国移动", "中国电信"],
        "overseas_leaders": ["Broadcom", "Arista", "Cisco", "NVIDIA", "Coherent", "Lumentum", "Amazon", "Microsoft"],
        "earnings_keywords": ["earnings", "results", "guidance", "capex", "订单", "出货", "财报", "指引", "扩产"],
        "event_keywords": ["订单", "扩产", "出货", "招标", "资本开支", "以太网", "800G", "1.6T"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 15, "overseas": 20, "news_density": 10, "news_heat": 10, "forward_event": 5},
    },
    "传媒": {
        "themes": ["传媒", "游戏", "AIGC"],
        "keywords": ["传媒", "游戏", "动漫", "影视", "aigc", "ai应用", "版号", "广告"],
        "policy_keywords": ["版号", "文化出海", "AIGC", "数字内容", "游戏"],
        "domestic_leaders": ["腾讯控股", "网易", "三七互娱", "恺英网络", "分众传媒", "完美世界"],
        "overseas_leaders": ["Nintendo", "Sony", "Netflix", "Meta", "Roblox"],
        "earnings_keywords": ["earnings", "results", "guidance", "流水", "财报", "指引", "版号"],
        "event_keywords": ["版号", "上线", "公测", "暑期档", "春节档", "出海"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 10, "overseas": 10, "news_density": 10, "news_heat": 10, "forward_event": 10},
    },
    "军工": {
        "themes": ["军工", "地缘风险", "国防"],
        "keywords": ["军工", "国防", "defense", "aerospace", "军贸", "无人机", "导弹", "卫星", "装备"],
        "policy_keywords": ["国防预算", "军费", "装备采购", "军贸", "国防", "军工", "军演", "安全"],
        "domestic_leaders": ["中航沈飞", "航发动力", "中航光电", "中航西飞", "洪都航空", "中国船舶", "中兵红箭"],
        "overseas_leaders": ["Lockheed Martin", "Northrop", "RTX", "General Dynamics", "Boeing", "Palantir"],
        "earnings_keywords": ["order", "guidance", "delivery", "财报", "订单", "交付", "指引"],
        "event_keywords": ["军演", "军贸", "订单", "交付", "首飞", "试飞", "卫星", "无人机"],
        "factor_max_overrides": {"policy": 25, "leader": 20, "structured": 15, "overseas": 10, "news_density": 10, "news_heat": 10, "forward_event": 10},
    },
    "能源": {
        "themes": ["能源", "原油", "通胀预期"],
        "keywords": ["能源", "oil", "gas", "lng", "opec", "原油", "天然气", "炼化"],
        "policy_keywords": ["能源安全", "产量", "增产", "减产", "战略储备"],
        "domestic_leaders": ["中国海油", "中国石油", "中国石化", "陕西煤业", "兖矿能源"],
        "overseas_leaders": ["Exxon", "Chevron", "Shell", "BP", "Saudi Aramco"],
        "earnings_keywords": ["earnings", "results", "production", "output", "财报", "产量", "指引"],
        "event_keywords": ["OPEC", "减产", "增产", "库存", "油价", "气价"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 15, "overseas": 15, "news_density": 10, "news_heat": 10, "forward_event": 10},
    },
    "高股息": {
        "themes": ["高股息", "防守", "红利"],
        "keywords": ["高股息", "红利", "dividend", "yield", "utility", "银行", "电信"],
        "policy_keywords": ["分红", "市值管理", "回购", "红利"],
        "domestic_leaders": ["中国神华", "长江电力", "中国移动", "工商银行", "农业银行"],
        "overseas_leaders": ["AT&T", "Verizon", "Duke Energy", "Coca-Cola"],
        "earnings_keywords": ["dividend", "buyback", "cash flow", "分红", "回购", "现金流"],
        "event_keywords": ["分红", "除权", "回购", "现金流"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 20, "overseas": 5, "news_density": 10, "news_heat": 10, "forward_event": 15},
    },
    "医药": {
        "themes": ["医药", "老龄化"],
        "keywords": ["医药", "biotech", "pharma", "drug", "医疗器械", "创新药"],
        "policy_keywords": ["医保", "集采", "审批", "创新药", "医疗"],
        "domestic_leaders": ["恒瑞医药", "迈瑞医疗", "药明康德", "爱尔眼科", "智飞生物"],
        "overseas_leaders": ["Eli Lilly", "Novo Nordisk", "Pfizer", "Merck", "AbbVie"],
        "earnings_keywords": ["trial", "approval", "guidance", "财报", "临床", "获批", "指引"],
        "event_keywords": ["临床", "获批", "医保谈判", "集采", "新药"],
        "factor_max_overrides": {"policy": 15, "leader": 20, "structured": 20, "overseas": 10, "news_density": 10, "news_heat": 10, "forward_event": 15},
    },
    "农业": {
        "themes": ["农业", "粮食安全", "种业"],
        "keywords": ["农业", "粮食", "grain", "seed", "种业", "化肥", "fertilizer", "农化", "农资", "food security"],
        "policy_keywords": ["粮食安全", "种业振兴", "乡村振兴", "耕地", "农资", "化肥", "农业"],
        "domestic_leaders": ["北大荒", "隆平高科", "大北农", "荃银高科", "农发种业", "云天化", "盐湖股份"],
        "overseas_leaders": ["Deere", "Corteva", "Nutrien", "Mosaic", "Bunge", "Archer Daniels Midland"],
        "earnings_keywords": ["harvest", "acreage", "fertilizer", "guidance", "crop", "财报", "种植", "库存", "价格", "指引"],
        "event_keywords": ["天气", "播种", "收割", "化肥", "粮价", "政策收储", "种业", "农资"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 15, "overseas": 10, "news_density": 10, "news_heat": 10, "forward_event": 15},
    },
    "消费": {
        "themes": ["消费", "内需"],
        "keywords": ["消费", "retail", "消费电子", "beer", "food", "旅游", "零售"],
        "policy_keywords": ["以旧换新", "促消费", "内需", "零售"],
        "domestic_leaders": ["贵州茅台", "美的集团", "海尔智家", "伊利股份", "中国中免"],
        "overseas_leaders": ["Nike", "Costco", "Walmart", "LVMH", "McDonald's"],
        "earnings_keywords": ["same-store", "guidance", "sales", "财报", "销售", "指引"],
        "event_keywords": ["促销", "补贴", "新品", "假期", "旺季"],
        "factor_max_overrides": {"policy": 15, "leader": 20, "structured": 20, "overseas": 10, "news_density": 10, "news_heat": 10, "forward_event": 10},
    },
    "宽基": {
        "themes": ["宽基", "大盘", "指数"],
        "keywords": ["index", "macro", "earnings", "rates", "liquidity", "指数", "宏观", "流动性"],
        "policy_keywords": ["利率", "财政", "政策", "流动性"],
        "domestic_leaders": ["工商银行", "贵州茅台", "宁德时代", "招商银行"],
        "overseas_leaders": ["Microsoft", "Apple", "NVIDIA", "Amazon", "Meta"],
        "earnings_keywords": ["earnings", "guidance", "rates", "payrolls", "cpi", "财报", "指引", "非农", "CPI"],
        "event_keywords": ["财报季", "利率决议", "CPI", "非农", "PMI"],
        "factor_max_overrides": {"policy": 20, "leader": 20, "structured": 15, "overseas": 15, "news_density": 10, "news_heat": 10, "forward_event": 10},
    },
}

CN_STOCK_CATALYST_OVERRIDE_PROFILES = {
    "科技",
    "军工",
    "能源",
    "消费",
    "宽基",
    "医药",
    "农业",
    "高股息",
    "半导体",
    "电网",
    "黄金",
    "有色",
}

CATALYST_CATEGORY_MAP = {
    "科技": {"ai", "earnings", "semiconductor", "fed", "global_macro"},
    "半导体": {"ai", "earnings", "semiconductor", "fed", "global_macro"},
    "通信": {"ai", "earnings", "semiconductor", "fed", "global_macro", "china_market_domestic"},
    "传媒": {"ai", "earnings", "china_macro", "china_market_domestic", "global_macro"},
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


def _chain_nodes_are_generic(chain_nodes: Sequence[str]) -> bool:
    cleaned = [str(item).strip() for item in chain_nodes if str(item).strip()]
    if not cleaned:
        return True
    return all(item in DEFAULT_CHAIN_NODES for item in cleaned)


def _chain_node_specificity_score(chain_nodes: Sequence[str], sector_hint: str = "") -> int:
    sector_text = str(sector_hint or "").strip()
    score = 0
    for item in [str(node).strip() for node in chain_nodes if str(node).strip()]:
        if item in DEFAULT_CHAIN_NODES or (sector_text and item == sector_text):
            continue
        score += 1
    return score


THEME_PROFILE_METADATA_KEYS = (
    "theme_family",
    "primary_chain",
    "theme_role",
    "theme_directness",
    "evidence_keywords",
    "preferred_sector_aliases",
    "mainline_tags",
    "taxonomy_profile_confidence",
)

THEME_CONFIRMATION_DIRECTNESS = {
    "direct",
    "adjacent",
    "application",
    "broad",
    "sidechain",
    "non_ai",
    "geopolitical",
}


def _taxonomy_terms(taxonomy: Mapping[str, Any]) -> List[str]:
    payload = dict(taxonomy or {})
    profile = dict(payload.get("theme_profile") or {})
    terms: List[str] = []
    for key in (
        "sector",
        "primary_chain",
        "theme_family",
        "theme_role",
        "theme_directness",
        "taxonomy_profile_confidence",
    ):
        value = str(payload.get(key, "") or profile.get(key, "")).strip()
        if value:
            terms.append(value)
    for key in ("chain_nodes", "labels", "evidence_keywords", "preferred_sector_aliases", "mainline_tags"):
        for value in list(payload.get(key) or profile.get(key) or []):
            text = str(value or "").strip()
            if text:
                terms.append(text)
    return _unique_strings(terms)


def _apply_theme_profile_metadata(metadata: Dict[str, Any]) -> Dict[str, Any]:
    merged = dict(metadata or {})
    taxonomy = dict(merged.get("taxonomy") or merged.get("fund_taxonomy") or {})
    profile = dict(taxonomy.get("theme_profile") or {})
    if not profile:
        profile = build_theme_taxonomy_profile(
            sector=str(merged.get("sector", "")),
            chain_nodes=[str(item).strip() for item in list(merged.get("chain_nodes") or []) if str(item).strip()],
            name=str(merged.get("name", "")),
            benchmark=str(merged.get("benchmark", "") or merged.get("benchmark_name", "") or merged.get("index_name", "")),
            tracking_target=str(merged.get("tracked_index_name", "") or merged.get("index_framework_label", "")),
            labels=list(taxonomy.get("labels") or []),
        )
        if taxonomy:
            taxonomy["theme_profile"] = profile
            for key in THEME_PROFILE_METADATA_KEYS:
                value = profile.get(key)
                if value not in (None, "", []):
                    taxonomy.setdefault(key, value)
            merged["taxonomy"] = taxonomy
    for key in THEME_PROFILE_METADATA_KEYS:
        value = taxonomy.get(key, profile.get(key))
        if value not in (None, "", []):
            merged.setdefault(key, value)
    if profile:
        merged.setdefault("theme_profile", profile)
    return merged


def _fund_like_theme_confirmation_gate(
    asset_type: str,
    metadata: Mapping[str, Any] | None,
    dimensions: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    if asset_type not in {"cn_etf", "cn_fund", "cn_index"}:
        return {"applies": False, "requires_confirmation": False, "warning": ""}

    payload = dict(metadata or {})
    directness = str(payload.get("theme_directness", "")).strip().lower()
    requires_confirmation = directness in THEME_CONFIRMATION_DIRECTNESS
    if not requires_confirmation:
        return {"applies": False, "requires_confirmation": False, "warning": ""}

    technical = int(dict(dimensions.get("technical") or {}).get("score") or 0)
    catalyst = int(dict(dimensions.get("catalyst") or {}).get("score") or 0)
    weak_technical = technical < 50
    weak_catalyst = catalyst < 20
    if not weak_technical and not weak_catalyst:
        return {"applies": False, "requires_confirmation": True, "warning": ""}

    label = (
        str(payload.get("primary_chain") or "").strip()
        or str(payload.get("theme_family") or "").strip()
        or str(payload.get("sector") or "").strip()
        or "主题线"
    )
    blockers: List[str] = []
    if weak_technical:
        blockers.append(f"技术面 `{technical}` 分")
    if weak_catalyst:
        blockers.append(f"催化面 `{catalyst}` 分")
    warning = f"⚠️ `{label}` 当前 {' / '.join(blockers)}，主题型产品先按观察处理，等价格与情报确认补齐后再升级。"
    return {
        "applies": True,
        "requires_confirmation": True,
        "warning": warning,
        "weak_technical": weak_technical,
        "weak_catalyst": weak_catalyst,
    }


def _preferred_match_rank(
    *,
    preferred: Sequence[str],
    sector: str = "",
    taxonomy: Mapping[str, Any] | None = None,
    metadata: Mapping[str, Any] | None = None,
) -> int:
    prefs = [str(item).strip() for item in preferred if str(item).strip()]
    if not prefs:
        return -1
    tokens = [str(sector or "").strip()]
    payloads = [dict(taxonomy or {}), dict(metadata or {})]
    for payload in payloads:
        tokens.extend(_taxonomy_terms(payload))
        nested_taxonomy = dict(payload.get("taxonomy") or payload.get("fund_taxonomy") or {})
        if nested_taxonomy:
            tokens.extend(_taxonomy_terms(nested_taxonomy))
        nested_profile = dict(payload.get("theme_profile") or nested_taxonomy.get("theme_profile") or {})
        tokens.extend(_taxonomy_terms(nested_profile))
    cleaned_tokens = _unique_strings([item for item in tokens if str(item).strip()])
    for index, pref in enumerate(prefs):
        for token in cleaned_tokens:
            if pref in token or token in pref:
                return index
    return -1


def _preserve_explicit_cn_stock_sector_label(
    current_sector: str,
    stock_industry_hint: str,
    normalized_sector: str,
) -> bool:
    sector_text = str(current_sector or "").strip()
    if not sector_text or sector_text in {"综合", "待分类", "未分类"}:
        return False
    if sector_text != str(stock_industry_hint or "").strip():
        return False
    if sector_text == str(normalized_sector or "").strip():
        return False
    # Preserve concrete equipment-style A-share industry labels like
    # "电气设备"/"电力设备", while still enriching chain_nodes via the broader
    # thematic sector mapping. Broader buckets such as "农林牧渔" should still
    # collapse back to the engine sector.
    return sector_text in _INDUSTRY_TO_SECTOR and "设备" in sector_text


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
    normalized_sector = ""
    normalized_chain_nodes: List[str] = []
    stock_industry_hint = str(merged.get("industry", "")).strip() or str(merged.get("sector", "")).strip()
    if asset_type == "cn_stock" and stock_industry_hint and stock_industry_hint not in {"综合", "待分类", "未分类"}:
        normalized_sector, normalized_chain_nodes = _map_industry_to_sector(
            stock_industry_hint,
            str(merged.get("name", symbol)).strip(),
        )
    if not normalized_sector or not normalized_chain_nodes:
        theme_text_parts = [
            merged.get("name", symbol),
            merged.get("sector", ""),
            merged.get("industry", ""),
            merged.get("industry_framework_label", ""),
            " ".join(str(item).strip() for item in merged.get("chain_nodes", []) if str(item).strip()),
            merged.get("main_business", ""),
            merged.get("business_scope", ""),
            merged.get("company_intro", ""),
        ]
        theme_text_blob = " ".join(str(item).strip() for item in theme_text_parts if str(item).strip())
        normalized_sector, normalized_chain_nodes = _normalize_sector(
            theme_text_blob,
            str(merged.get("sector", "综合")),
        )
        if (
            asset_type == "cn_stock"
            and normalized_sector == "电网"
            and any(token in theme_text_blob for token in _POWER_EQUIPMENT_STOCK_KEYWORDS)
        ):
            normalized_sector = "电力设备"
            normalized_chain_nodes = list(_POWER_EQUIPMENT_STOCK_CHAIN_NODES)
    current_sector = str(merged.get("sector", "")).strip()
    known_sectors = _known_sector_buckets()
    preserve_explicit_stock_sector = (
        asset_type == "cn_stock"
        and _preserve_explicit_cn_stock_sector_label(
            current_sector,
            stock_industry_hint,
            normalized_sector,
        )
    )
    if (
        not current_sector
        or current_sector in {"综合", "待分类", "未分类"}
        or (
            current_sector not in known_sectors
            and normalized_sector in known_sectors
            and not preserve_explicit_stock_sector
        )
    ):
        merged["sector"] = normalized_sector
    else:
        merged.setdefault("sector", normalized_sector)
    existing_chain_nodes = [str(item).strip() for item in merged.get("chain_nodes", []) if str(item).strip()]
    if _chain_nodes_are_generic(existing_chain_nodes):
        merged["chain_nodes"] = normalized_chain_nodes
    else:
        merged.setdefault("chain_nodes", normalized_chain_nodes)
    if not str(merged.get("industry_framework_label", "")).strip():
        if asset_type == "cn_stock" and str(merged.get("industry", "")).strip():
            merged["industry_framework_label"] = str(merged.get("industry", "")).strip()
        elif asset_type in {"cn_etf", "cn_fund", "cn_index"}:
            fund_exposure_label = next(
                (
                    str(merged.get(key, "")).strip()
                    for key in ("tracked_index_name", "benchmark_name", "benchmark", "index_name")
                    if str(merged.get(key, "")).strip()
                ),
                "",
            )
            if fund_exposure_label:
                merged["industry_framework_label"] = fund_exposure_label
            elif str(merged.get("sector", "")).strip() not in {"", "综合", "待分类", "未分类"}:
                merged["industry_framework_label"] = str(merged.get("sector", "")).strip()
        else:
            lead_chain = next((item for item in merged.get("chain_nodes", []) if str(item).strip()), "")
            if lead_chain:
                merged["industry_framework_label"] = str(lead_chain).strip()
    merged.setdefault("region", {"cn_etf": "CN", "cn_stock": "CN", "hk": "HK", "hk_index": "HK", "us": "US", "futures": "CN"}.get(asset_type, "CN"))
    return _apply_theme_profile_metadata(merged)


def _collect_fund_profile(symbol: str, asset_type: str, config: Mapping[str, Any]) -> Dict[str, Any]:
    if not symbol:
        return {}
    effective_config = dict(config or {})
    if bool(effective_config.get("skip_fund_profile")):
        return {}
    profile_mode = str(effective_config.get("fund_profile_mode", "") or "").strip().lower() or "full"
    etf_profile_mode = str(effective_config.get("etf_fund_profile_mode", "") or "").strip().lower()
    if asset_type == "cn_etf" and etf_profile_mode:
        profile_mode = etf_profile_mode
    timeout_seconds = float(effective_config.get("fund_profile_timeout_seconds", 0) or 0)
    try:
        return _timed_runtime_loader(
            lambda: FundProfileCollector(config).collect_profile(symbol, asset_type=asset_type, profile_mode=profile_mode),
            timeout_seconds=timeout_seconds,
            fallback={
                "overview": {},
                "achievement": {},
                "top_holdings": [],
                "industry_allocation": [],
                "asset_allocation": {},
                "manager": {},
                "company": {},
                "dividends": {},
                "rating": {},
                "style": {},
                "latest_quarter": "",
                "etf_snapshot": {},
                "fund_factor_snapshot": {},
                "profile_mode": profile_mode,
                "notes": [
                    f"基金画像拉取超时（>{int(timeout_seconds)}s），本轮按缺失处理。"
                ] if timeout_seconds > 0 else [],
                "timeout": timeout_seconds > 0,
            },
        )
    except Exception:
        return {}


def _enrich_metadata_with_fund_profile(metadata: Dict[str, Any], fund_profile: Mapping[str, Any]) -> Dict[str, Any]:
    if not fund_profile:
        return metadata
    enriched = dict(metadata)
    overview = dict(fund_profile.get("overview") or {})
    style = dict(fund_profile.get("style") or {})
    etf_snapshot = dict(fund_profile.get("etf_snapshot") or {})
    fund_name = str(overview.get("基金简称", "")).strip()
    if fund_name:
        enriched["name"] = fund_name
    sector = str(style.get("sector", "")).strip()
    if sector and sector != "综合":
        enriched["sector"] = sector
    chain_nodes = list(style.get("chain_nodes") or [])
    if chain_nodes:
        existing_nodes = [str(item).strip() for item in enriched.get("chain_nodes", []) if str(item).strip()]
        incoming_nodes = [str(item).strip() for item in chain_nodes if str(item).strip()]
        sector_hint = str(sector or enriched.get("sector", "")).strip()
        existing_specificity = _chain_node_specificity_score(existing_nodes, sector_hint=sector_hint)
        incoming_specificity = _chain_node_specificity_score(incoming_nodes, sector_hint=sector_hint)
        if not existing_nodes:
            enriched["chain_nodes"] = incoming_nodes
        elif existing_specificity == 0 and incoming_specificity > 0:
            enriched["chain_nodes"] = incoming_nodes
        elif existing_specificity > incoming_specificity:
            enriched["chain_nodes"] = existing_nodes
        elif incoming_specificity > existing_specificity:
            enriched["chain_nodes"] = incoming_nodes
        else:
            enriched["chain_nodes"] = _unique_strings([*existing_nodes, *incoming_nodes])
    tags = [str(item).strip() for item in style.get("tags") or [] if str(item).strip()]
    if tags:
        enriched["fund_style_tags"] = tags
        enriched["is_passive_fund"] = "被动跟踪" in tags
    taxonomy = dict(style.get("taxonomy") or {})
    if taxonomy:
        enriched["fund_taxonomy"] = taxonomy
        enriched["fund_taxonomy_labels"] = list(taxonomy.get("labels") or [])
        enriched["fund_management_style"] = str(taxonomy.get("management_style", ""))
        enriched["fund_exposure_scope"] = str(taxonomy.get("exposure_scope", ""))
        for key in THEME_PROFILE_METADATA_KEYS:
            value = taxonomy.get(key) or dict(taxonomy.get("theme_profile") or {}).get(key)
            if value not in (None, "", []):
                enriched[key] = value
    benchmark = str(overview.get("业绩比较基准", "")).strip()
    if benchmark:
        enriched["benchmark"] = benchmark
    manager_name = str(overview.get("基金经理人", "")).strip()
    if manager_name:
        enriched["manager_name"] = manager_name
    fund_factor_snapshot = dict(fund_profile.get("fund_factor_snapshot") or {})
    if fund_factor_snapshot:
        enriched["fund_factor_snapshot"] = fund_factor_snapshot
        for source_key, target_key in (
            ("trend_label", "fund_factor_trend_label"),
            ("momentum_label", "fund_factor_momentum_label"),
            ("signal_strength", "fund_factor_signal_strength"),
            ("trade_date", "fund_factor_trade_date"),
        ):
            value = fund_factor_snapshot.get(source_key)
            if value not in (None, "", []):
                enriched[target_key] = value
    for source_key, target_key in (
        ("index_code", "index_code"),
        ("index_name", "index_name"),
        ("index_name", "benchmark_name"),
        ("exchange", "exchange"),
        ("list_status", "list_status"),
        ("etf_type", "etf_type"),
        ("total_share", "total_share"),
        ("total_size", "total_size"),
        ("share_as_of", "share_as_of"),
        ("etf_share_change", "etf_share_change"),
        ("etf_share_change_pct", "etf_share_change_pct"),
        ("etf_size_change", "etf_size_change"),
        ("etf_size_change_pct", "etf_size_change_pct"),
    ):
        value = etf_snapshot.get(source_key)
        if value not in (None, "", []):
                enriched[target_key] = value
    sales_ratio_snapshot = dict(fund_profile.get("sales_ratio_snapshot") or {})
    if sales_ratio_snapshot:
        enriched["sales_ratio_snapshot"] = sales_ratio_snapshot
        if sales_ratio_snapshot.get("lead_channel") not in (None, "", []):
            enriched["sales_ratio_lead_channel"] = str(sales_ratio_snapshot.get("lead_channel", "")).strip()
        if sales_ratio_snapshot.get("latest_year") not in (None, "", []):
            enriched["sales_ratio_latest_year"] = str(sales_ratio_snapshot.get("latest_year", "")).strip()
    if etf_snapshot.get("index_name") not in (None, "", []) and not str(enriched.get("benchmark", "")).strip():
        enriched["benchmark"] = str(etf_snapshot.get("index_name", "")).strip()
    if etf_snapshot.get("manager_name") not in (None, "", []):
        enriched.setdefault("management", str(etf_snapshot.get("manager_name", "")).strip())
    return _apply_theme_profile_metadata(enriched)


def _enrich_metadata_with_industry_index_snapshot(
    metadata: Dict[str, Any],
    snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    if not snapshot:
        return metadata
    enriched = dict(metadata)
    items = [dict(item) for item in list(snapshot.get("items") or []) if dict(item)]
    if not items:
        return enriched
    preferred_labels = [
        str(item.get("index_name", "")).strip()
        for item in items
        if str(item.get("level", "")).strip() in {"L3", "L2"} and str(item.get("index_name", "")).strip()
    ]
    if not preferred_labels:
        preferred_labels = [
            str(item.get("index_name", "")).strip()
            for item in items
            if str(item.get("index_name", "")).strip()
        ]
    current_chain = [str(item).strip() for item in enriched.get("chain_nodes", []) if str(item).strip()]
    for label in preferred_labels[:4]:
        if label and label not in current_chain:
            current_chain.append(label)
    if current_chain:
        enriched["chain_nodes"] = current_chain
    enriched["industry_index_snapshot"] = {
        key: value
        for key, value in dict(snapshot).items()
        if key != "families"
    }
    if not str(enriched.get("industry_framework_label", "")).strip():
        lead = next((item for item in preferred_labels if item), "")
        if lead:
            enriched["industry_framework_label"] = lead
    return enriched


def _enrich_metadata_with_stock_theme_membership(
    metadata: Dict[str, Any],
    snapshot: Mapping[str, Any],
) -> Dict[str, Any]:
    if not snapshot:
        return metadata
    enriched = dict(metadata)
    items = [dict(item) for item in list(snapshot.get("items") or []) if dict(item)]
    if not items:
        return enriched
    current_chain = [str(item).strip() for item in enriched.get("chain_nodes", []) if str(item).strip()]
    labels: list[str] = []
    industry_labels: list[str] = []
    for item in items[:4]:
        board_name = str(item.get("board_name", "")).strip()
        board_type = str(item.get("board_type", "")).strip()
        if not board_name:
            continue
        labels.append(board_name)
        if board_type == "industry":
            industry_labels.append(board_name)
        if board_type == "concept" and board_name not in current_chain:
            current_chain.append(board_name)
    if current_chain:
        enriched["chain_nodes"] = current_chain
    if labels:
        enriched["tushare_theme_membership_labels"] = labels
    if industry_labels:
        enriched["tushare_theme_industry"] = industry_labels[0]
        if not str(enriched.get("industry_framework_label", "")).strip():
            enriched["industry_framework_label"] = industry_labels[0]
    enriched["stock_theme_membership"] = {
        key: value
        for key, value in dict(snapshot).items()
        if key != "items"
    }
    return enriched


def _analysis_theme_playbook_context(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    fund_profile: Mapping[str, Any] | None = None,
    narrative: Mapping[str, Any] | None = None,
    notes: Sequence[str] | None = None,
) -> Dict[str, Any]:
    day_theme = dict(context.get("day_theme") or {})
    alignment_level, aligned_day_theme = _theme_alignment_match(metadata, day_theme)
    if alignment_level != "direct":
        aligned_day_theme = ""
    metadata_context = {
        "name": metadata.get("name"),
        "symbol": metadata.get("symbol"),
        "sector": metadata.get("sector"),
        "industry": metadata.get("industry"),
        "industry_framework_label": metadata.get("industry_framework_label"),
        "tushare_theme_industry": metadata.get("tushare_theme_industry"),
        "tushare_theme_membership_labels": metadata.get("tushare_theme_membership_labels"),
        "chain_nodes": metadata.get("chain_nodes"),
        "main_business": metadata.get("main_business"),
        "benchmark": metadata.get("benchmark"),
        "benchmark_name": metadata.get("benchmark_name"),
        "tracked_index_name": metadata.get("tracked_index_name"),
        "index_name": metadata.get("index_name"),
        "index_framework_label": metadata.get("index_framework_label"),
    }
    return build_theme_playbook_context(
        metadata_context,
        aligned_day_theme,
        dict(fund_profile or {}).get("overview", {}).get("业绩比较基准", ""),
        dict(fund_profile or {}).get("style", {}).get("summary", ""),
        dict(fund_profile or {}).get("style", {}).get("benchmark_note", ""),
    )


FUND_BENCHMARK_HINTS = [
    (("战略新兴", "新兴产业"), ["战略新兴", "新兴产业"]),
    (("恒生科技", "港股科技", "港股通科技"), ["恒生科技", "港股科技", "港股通科技"]),
    (("沪深300",), ["沪深300"]),
    (("中证a500", "a500"), ["中证A500"]),
    (("中证500",), ["中证500"]),
    (("创业板",), ["创业板"]),
    (("科创50",), ["科创50"]),
    (("半导体", "芯片"), ["半导体", "芯片"]),
    (("军工", "国防"), ["军工", "国防"]),
    (("农业", "粮食", "种业", "农化", "国证粮食"), ["农业", "粮食", "种业", "农化", "国证粮食"]),
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
    (("农", "林", "牧", "渔", "农业", "种植", "种业", "化肥"), ["农业", "粮食", "种业", "农化"]),
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

SEMICONDUCTOR_VALUATION_NOISY_KEYWORDS = {
    "ai",
    "AI",
    "人工智能",
    "科技",
    "战略新兴",
    "信息技术",
    "通信",
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


def _dimension_summary(
    score: Optional[int],
    positive: str,
    neutral: str,
    negative: str,
    missing: str,
    *,
    max_score: int = 100,
) -> str:
    if score is None:
        return missing
    if score >= int(round(max_score * 0.70)):
        return positive
    if score >= int(round(max_score * 0.40)):
        return neutral
    return negative


def _normalize_dimension(raw_score: int, available_max: int, target_max: int) -> Optional[int]:
    if available_max <= 0:
        return None
    normalized = int(round(raw_score / available_max * target_max))
    return max(0, min(target_max, normalized))


def _structured_source_priority(item: Mapping[str, Any]) -> int:
    source = str(item.get("source") or item.get("configured_source") or "").strip().lower()
    if source.startswith("tushare"):
        return 3
    if any(token in source for token in ("investor relations", "hkex", "sec", "cninfo")):
        return 3
    if source in {"business wire", "pr newswire", "globenewswire"}:
        return 2
    if source in {"reuters", "bloomberg", "financial times", "ft", "财联社", "证券时报"}:
        return 2
    return 1 if source else 0


def _structured_event_award(
    item: Mapping[str, Any],
    reference: datetime,
    *,
    strong_event: bool,
) -> tuple[int, str]:
    event_date = _item_datetime(item, reference)
    if event_date is None:
        fallback = 15 if strong_event else 8
        return fallback, "未提取到明确事件日期；若属于直接公司公告/事件，先按高置信结构化事件处理。"
    delta_days = (reference.date() - event_date.date()).days
    if delta_days < 0:
        days_ahead = abs(delta_days)
        return (8 if strong_event else 5), f"事件发生在未来 {days_ahead} 天，先按前瞻结构化事件处理。"
    if delta_days <= STRUCTURED_EVENT_FULL_SCORE_DAYS:
        return (15 if strong_event else 8), f"事件距今 {delta_days} 天，仍在结构化事件高权重窗口内。"
    if delta_days <= STRUCTURED_EVENT_DECAY_DAYS:
        return (8 if strong_event else 5), f"事件距今 {delta_days} 天，已进入结构化事件衰减窗口。"
    return 0, f"事件距今 {delta_days} 天，已超出结构化事件有效窗口，不再作为当前催化加分。"


def _pick_best_structured_item(
    items: Sequence[Mapping[str, Any]],
    primary_keywords: Sequence[str],
    bonus_keywords: Sequence[str],
    reference: datetime,
) -> Optional[Mapping[str, Any]]:
    if not items:
        return None

    def _freshness_rank(item: Mapping[str, Any]) -> tuple[int, int]:
        event_date = _item_datetime(item, reference)
        if event_date is None:
            return (0, -9999)
        delta_days = (reference.date() - event_date.date()).days
        abs_days = abs(delta_days)
        if delta_days < 0:
            return (2, -abs_days)
        if delta_days <= STRUCTURED_EVENT_FULL_SCORE_DAYS:
            return (3, -delta_days)
        if delta_days <= STRUCTURED_EVENT_DECAY_DAYS:
            return (1, -delta_days)
        return (0, -delta_days)

    def _score(item: Mapping[str, Any]) -> tuple[int, int, int, int, int]:
        text = _headline_text(item)
        primary = sum(1 for keyword in primary_keywords if str(keyword).strip() and str(keyword).lower() in text)
        bonus = sum(1 for keyword in bonus_keywords if str(keyword).strip() and str(keyword).lower() in text)
        freshness_bucket, freshness_days = _freshness_rank(item)
        return (
            freshness_bucket,
            _structured_source_priority(item),
            primary,
            bonus,
            freshness_days,
        )

    return max(items, key=_score)


def _top_positive_signals(factors: Sequence[Dict[str, str]], limit: int = 3) -> str:
    positives = [item["signal"] for item in factors if item.get("awarded", 0) > 0]
    return " · ".join(positives[:limit]) if positives else "当前没有新增直接情报亮点"


def _top_material_signals(
    factors: Sequence[Dict[str, Any]],
    *,
    positive_limit: int = 2,
    negative_limit: int = 1,
) -> str:
    positives: List[tuple[float, str]] = []
    negatives: List[tuple[float, str]] = []
    for item in factors:
        signal = str(item.get("signal", "")).strip()
        if not signal or signal == "缺失":
            continue
        try:
            awarded = float(item.get("awarded", 0) or 0)
        except (TypeError, ValueError):
            awarded = 0.0
        if awarded > 0:
            positives.append((awarded, signal))
        elif awarded < 0:
            negatives.append((abs(awarded), signal))
    positives.sort(key=lambda row: row[0], reverse=True)
    negatives.sort(key=lambda row: row[0], reverse=True)
    parts = [signal for _, signal in negatives[: max(int(negative_limit), 0)]]
    parts.extend(signal for _, signal in positives[: max(int(positive_limit), 0)])
    return " · ".join(parts) if parts else _top_positive_signals(factors)


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
    catalyst_coverage: Mapping[str, Any] | None = None,
) -> str:
    coverage = dict(catalyst_coverage or {})
    diagnosis = str(coverage.get("diagnosis", "")).strip()
    negative_signal = next(
        (
            str(item.get("signal", "")).strip()
            for item in factors
            if str(item.get("display_score", "")).strip().startswith("-")
            and item.get("name") in {"负面事件", "主题逆风"}
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
        if diagnosis == "suspected_search_gap":
            return "当前新增直接情报偏少，且主题检索疑似漏抓；先不把它写成零催化"
        if diagnosis == "proxy_degraded":
            return "当前新增直接情报偏少，且本轮覆盖有降级；先按低置信观察处理"
        if diagnosis == "theme_only_live":
            return "当前只有主题级情报，个股级新增证据还不够"
        if diagnosis == "stale_live_only":
            return "当前能看到的多是旧闻回放，新增直接情报仍待确认"
    return _top_positive_signals(factors)


def _factor_row(
    name: str,
    signal: str,
    awarded: Optional[int],
    maximum: int,
    detail: str,
    display_score: Optional[str] = None,
    factor_id: Optional[str] = None,
    factor_meta_overrides: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    meta = factor_meta_payload(str(factor_id).strip(), overrides=factor_meta_overrides) if factor_id else {}
    return {
        "name": name,
        "signal": signal,
        "awarded": awarded if awarded is not None else 0,
        "max": maximum,
        "detail": detail,
        "display_score": display_score or ("缺失" if awarded is None else f"{awarded}/{maximum}"),
        "factor_id": factor_id,
        "factor_meta": meta,
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


def _parse_rank_fraction(value: Any) -> tuple[Optional[int], Optional[int]]:
    match = re.search(r"(\d+)\s*/\s*(\d+)", str(value or "").strip())
    if not match:
        return None, None
    rank = int(match.group(1))
    total = int(match.group(2))
    if rank <= 0 or total <= 0 or rank > total:
        return None, None
    return rank, total


def _fund_recent_achievement_snapshot(
    context: Mapping[str, Any],
    *,
    period_aliases: Sequence[str] = FUND_ACHIEVEMENT_3M_ALIASES,
) -> Dict[str, Any]:
    fund_profile = dict(context.get("fund_profile") or {})
    achievement = dict(fund_profile.get("achievement") or {})
    if not achievement:
        return {}

    period_label = ""
    entry: Dict[str, Any] = {}
    for alias in period_aliases:
        if alias in achievement:
            period_label = alias
            entry = dict(achievement.get(alias) or {})
            break
    if not entry:
        for key, value in achievement.items():
            normalized_key = str(key).replace("个月", "月")
            if "3月" in normalized_key or "三月" in normalized_key:
                period_label = str(key)
                entry = dict(value or {})
                break
    if not entry:
        return {}

    rank, total = _parse_rank_fraction(entry.get("peer_rank"))
    return_pct_series = pd.to_numeric(pd.Series([entry.get("return_pct")]), errors="coerce").dropna()
    return_pct = float(return_pct_series.iloc[0]) if not return_pct_series.empty else None
    if return_pct is not None and abs(return_pct) > 1.5:
        return_pct = return_pct / 100.0
    return {
        "period_label": period_label,
        "peer_rank_text": str(entry.get("peer_rank", "")).strip(),
        "rank": rank,
        "total": total,
        "percentile": (float(rank) / float(total)) if rank and total else None,
        "return_pct": return_pct,
    }


def _business_day_gap(start: Any, end: Any) -> Optional[int]:
    start_ts = pd.to_datetime(start, errors="coerce")
    end_ts = pd.to_datetime(end, errors="coerce")
    if pd.isna(start_ts) or pd.isna(end_ts):
        return None
    start_day = start_ts.normalize()
    end_day = end_ts.normalize()
    if end_day <= start_day:
        return 0
    return max(len(pd.bdate_range(start_day, end_day)) - 1, 0)


def _divergence_signal_age_days(divergence: Mapping[str, Any], latest_date: Any) -> Optional[int]:
    hits = list(divergence.get("hits") or [])
    latest_hit = None
    for hit in hits:
        hit_date = pd.to_datetime(dict(hit).get("current_date"), errors="coerce")
        if pd.isna(hit_date):
            continue
        latest_hit = hit_date if latest_hit is None else max(latest_hit, hit_date)
    if latest_hit is None:
        return None
    return _business_day_gap(latest_hit, latest_date)


def _estimated_natr(history: pd.DataFrame, technical: Mapping[str, Any]) -> float:
    volatility = dict(technical.get("volatility") or {})
    natr_series = pd.to_numeric(pd.Series([volatility.get("NATR")]), errors="coerce").dropna()
    if not natr_series.empty:
        return max(float(natr_series.iloc[0]), 0.0)

    if history is None or history.empty:
        return 0.0
    normalized = normalize_ohlcv_frame(history)
    if len(normalized) < 2:
        return 0.0
    high = normalized["high"].astype(float)
    low = normalized["low"].astype(float)
    close = normalized["close"].astype(float)
    prev_close = close.shift(1)
    true_range = pd.concat(
        [
            (high - low),
            (high - prev_close).abs(),
            (low - prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1, skipna=True).dropna()
    if true_range.empty or float(close.iloc[-1]) <= 0:
        return 0.0
    atr = float(true_range.tail(min(len(true_range), 14)).mean())
    return max(atr / float(close.iloc[-1]), 0.0)


def _minimum_stop_gap_from_atr(history: pd.DataFrame, technical: Mapping[str, Any], *, asset_type: str = "") -> float:
    if asset_type not in {"cn_etf", "cn_fund", "cn_index"}:
        return 0.0
    natr = _estimated_natr(history, technical)
    if natr <= 0:
        return 0.0
    return min(max(natr * 2.0, 0.0), 0.25)


def _safe_history(symbol: str, asset_type: str, config: Mapping[str, Any], period: str = "3y") -> Optional[pd.DataFrame]:
    try:
        return normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config), period=period))
    except Exception:
        return None


def _monitor_map(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Mapping[str, Any]]:
    return {str(item.get("name", "")): item for item in rows}


def _fresh_market_frame(source: Mapping[str, Any], key: str) -> pd.DataFrame:
    report = dict(source.get(f"{key}_report") or {})
    if report.get("frame_empty") or (report and report.get("is_fresh") is False):
        return pd.DataFrame()
    frame = source.get(key, pd.DataFrame())
    if not isinstance(frame, pd.DataFrame):
        return pd.DataFrame()
    if frame.empty:
        return pd.DataFrame()
    if getattr(frame, "attrs", {}).get("is_fresh") is False:
        return pd.DataFrame()
    return frame


def _top_market_labels(frame: pd.DataFrame, *columns: str, limit: int = 3) -> List[str]:
    if frame is None or frame.empty:
        return []
    rows: List[str] = []
    for column in columns:
        if column not in frame.columns:
            continue
        for value in frame[column].tolist():
            text = str(value or "").strip()
            if not text or text.lower() == "nan" or text in rows:
                continue
            rows.append(text)
            if len(rows) >= limit:
                return rows
    return rows


def _today_theme_structure_text(
    drivers: Mapping[str, Any] | None = None,
    pulse: Mapping[str, Any] | None = None,
) -> str:
    driver_payload = dict(drivers or {})
    pulse_payload = dict(pulse or {})
    parts: List[str] = []
    parts.extend(_top_market_labels(_fresh_market_frame(pulse_payload, "zt_pool"), "所属行业", limit=3))
    parts.extend(_top_market_labels(_fresh_market_frame(pulse_payload, "strong_pool"), "所属行业", limit=3))
    parts.extend(_top_market_labels(_fresh_market_frame(driver_payload, "industry_spot"), "名称", limit=3))
    parts.extend(_top_market_labels(_fresh_market_frame(driver_payload, "concept_spot"), "名称", limit=3))
    parts.extend(_top_market_labels(_fresh_market_frame(driver_payload, "hot_rank"), "概念名称", "名称", limit=3))
    ordered = list(dict.fromkeys(part for part in parts if part))
    return " ".join(ordered)


DAY_THEME_LABELS = {
    "rate_growth": "利率驱动成长修复",
    "ai_semis": "硬科技 / AI硬件链",
    "power_utilities": "电网 / 公用事业",
    "china_policy": "中国政策 / 内需确定性",
    "broad_market_repair": "宽基修复",
    "innovation_medicine": "创新药 / 医药催化",
}


def _theme_keyword_hits(labels: Sequence[str], keywords: Sequence[str]) -> int:
    hits = 0
    for label in labels:
        text = str(label or "").strip()
        if text and any(keyword in text for keyword in keywords):
            hits += 1
    return hits


def _theme_headline_hits(items: Sequence[Mapping[str, Any]], keywords: Sequence[str]) -> int:
    lowered_keywords = [str(keyword).strip().lower() for keyword in keywords if str(keyword).strip()]
    if not lowered_keywords:
        return 0
    hits = 0
    for item in items:
        text = " ".join(
            [
                str(item.get("title", "") or ""),
                str(item.get("category", "") or ""),
                str(item.get("source", "") or ""),
            ]
        ).lower()
        if text and any(keyword in text for keyword in lowered_keywords):
            hits += 1
    return hits


def _headline_hit_score(hits: int) -> int:
    count = max(int(hits or 0), 0)
    if count >= 4:
        return 4
    return count


def _day_theme_labels(day_theme: Mapping[str, Any] | None) -> List[str]:
    theme = dict(day_theme or {})
    labels: List[str] = []
    primary = str(theme.get("label", "")).strip()
    if primary:
        labels.append(primary)
    for item in theme.get("secondary_labels", []) or []:
        label = str(item).strip()
        if label and label not in labels:
            labels.append(label)
    return labels


def _market_context_watch_hint_lines(
    watchlist: Sequence[Mapping[str, Any]] | None = None,
    watchlist_returns: Mapping[str, pd.Series] | None = None,
    *,
    limit: int = 6,
) -> List[str]:
    best_by_sector: Dict[str, tuple[float, float, str]] = {}
    returns_map = dict(watchlist_returns or {})
    for item in watchlist or []:
        symbol = str(item.get("symbol", "")).strip()
        returns = returns_map.get(symbol)
        if not isinstance(returns, pd.Series) or returns.dropna().empty:
            continue
        return_1d = _window_return(returns, 1)
        return_5d = _window_return(returns, 5)
        if return_1d <= 0 and return_5d <= 0:
            continue
        sector = str(item.get("sector", "")).strip() or "综合"
        parts = [
            sector,
            *[str(node).strip() for node in list(item.get("chain_nodes") or [])[:2] if str(node).strip()],
        ]
        text = " ".join(dict.fromkeys(part for part in parts if part)).strip()
        if text:
            previous = best_by_sector.get(sector)
            candidate = (return_1d, return_5d, text)
            if previous is None or candidate > previous:
                best_by_sector[sector] = candidate
    hints: List[str] = []
    for _return_1d, _return_5d, text in sorted(best_by_sector.values(), reverse=True):
        if text not in hints:
            hints.append(text)
        if len(hints) >= max(int(limit), 1):
            break
    return hints


def _market_context_hint_lines(
    drivers: Mapping[str, Any] | None = None,
    pulse: Mapping[str, Any] | None = None,
    *,
    watchlist: Sequence[Mapping[str, Any]] | None = None,
    watchlist_returns: Mapping[str, pd.Series] | None = None,
) -> List[str]:
    hints: List[str] = []
    for item in _market_context_watch_hint_lines(watchlist, watchlist_returns):
        if item not in hints:
            hints.append(item)
    structure_text = _today_theme_structure_text(drivers, pulse).strip()
    if structure_text and structure_text not in hints:
        hints.append(structure_text)
    return hints


def _merge_market_context_intel_reports(
    reports: Sequence[Mapping[str, Any]] | None = None,
    *,
    limit: int = 6,
) -> Dict[str, Any]:
    merged_items: List[Dict[str, Any]] = []
    merged_all_items: List[Dict[str, Any]] = []
    source_list: List[str] = []
    summary_lines: List[str] = []
    note_parts: List[str] = []
    disclosure = ""

    for report in reports or []:
        payload = dict(report or {})
        selected_items = [dict(item) for item in list(payload.get("items") or []) if isinstance(item, Mapping)]
        ranked_items = [
            dict(item) for item in list(payload.get("all_items") or payload.get("items") or []) if isinstance(item, Mapping)
        ]
        has_hits = bool(selected_items or ranked_items)
        merged_items.extend(selected_items)
        merged_all_items.extend(ranked_items)
        for source in list(payload.get("source_list") or []):
            text = str(source).strip()
            if text and text not in source_list:
                source_list.append(text)
        for line in list(payload.get("summary_lines") or []):
            text = str(line).strip()
            if text and text not in summary_lines:
                summary_lines.append(text)
        note = str(payload.get("note") or "").strip()
        if note and note not in note_parts and (has_hits or not note_parts):
            note_parts.append(note)
        if not disclosure:
            disclosure = str(payload.get("disclosure") or "").strip()

    deduped_items = [dict(item) for item in _dedupe_news_items(merged_items)]
    deduped_all_items = [dict(item) for item in _dedupe_news_items(merged_all_items or merged_items)]
    if not deduped_items and not deduped_all_items:
        return {}
    if not deduped_items:
        deduped_items = deduped_all_items[: max(int(limit), 1)]
    lines = [
        str(item.get("title") or "").strip()
        for item in deduped_items[: max(int(limit), 1)]
        if str(item.get("title") or "").strip()
    ]
    items = deduped_items[: max(int(limit), 1)]
    all_items = deduped_all_items[: max(int(limit) * 4, len(deduped_all_items), 1)]
    return {
        "mode": "live" if items else "proxy",
        "items": items,
        "all_items": all_items,
        "summary_lines": summary_lines[:4],
        "lead_summary": summary_lines[0] if summary_lines else "",
        "lines": lines,
        "source_list": source_list,
        "note": " ".join(note_parts).strip(),
        "disclosure": disclosure,
    }


def _today_theme_watch_boosts(
    watchlist: Sequence[Mapping[str, Any]] | None = None,
    watchlist_returns: Mapping[str, pd.Series] | None = None,
) -> Dict[str, int]:
    boosts = {
        "ai_semis": 0,
        "power_utilities": 0,
        "china_policy": 0,
        "broad_market_repair": 0,
        "innovation_medicine": 0,
    }
    returns_map = dict(watchlist_returns or {})
    for item in watchlist or []:
        symbol = str(item.get("symbol", "")).strip()
        returns = returns_map.get(symbol)
        if not isinstance(returns, pd.Series) or returns.dropna().empty:
            continue
        return_1d = _window_return(returns, 1)
        return_5d = _window_return(returns, 5)
        strength = 0
        if return_1d > 0.015:
            strength += 2
        elif return_1d > 0:
            strength += 1
        if return_5d > 0.04:
            strength += 2
        elif return_5d > 0.01:
            strength += 1
        if strength <= 0:
            continue
        text = " ".join(
            [
                str(item.get("name", "") or ""),
                str(item.get("sector", "") or ""),
                " ".join(str(node).strip() for node in item.get("chain_nodes", []) if str(node).strip()),
                str(item.get("notes", "") or ""),
            ]
        ).lower()
        if any(token in text for token in ("半导体", "芯片", "通信", "通信设备", "光模块", "cpo", "算力", "人工智能", "ai", "液冷", "存储", "pcb")):
            boosts["ai_semis"] += strength + (
                1 if any(token in text for token in ("cpo", "光模块", "通信设备", "半导体", "芯片")) else 0
            )
        if any(token in text for token in ("电网", "电力", "公用事业", "特高压", "储能", "逆变器")):
            boosts["power_utilities"] += strength
            boosts["china_policy"] += max(1, strength - 1)
        if any(token in text for token in ("创新药", "医药", "biotech", "pharma", "license-out", "bd", "临床")):
            boosts["innovation_medicine"] += strength + (
                1 if any(token in text for token in ("创新药", "license-out", "bd", "临床")) else 0
            )
        if any(token in text for token in ("宽基", "沪深300", "a500", "银行", "券商", "红利")):
            boosts["broad_market_repair"] += strength
    return boosts


def _today_theme(
    news_report: Mapping[str, Any],
    monitor_rows: Sequence[Mapping[str, Any]],
    *,
    drivers: Mapping[str, Any] | None = None,
    pulse: Mapping[str, Any] | None = None,
    watchlist: Sequence[Mapping[str, Any]] | None = None,
    watchlist_returns: Mapping[str, pd.Series] | None = None,
) -> Dict[str, Any]:
    items = list(news_report.get("items", []) or news_report.get("all_items", []) or [])
    counter: Counter[str] = Counter(str(item.get("category", "")).lower() for item in items if item.get("category"))
    monitor = _monitor_map(monitor_rows)
    brent_5d = float(monitor.get("布伦特原油", {}).get("return_5d", 0.0))
    vix = float(monitor.get("VIX波动率", {}).get("latest", 0.0))
    structure_labels = list(dict.fromkeys(_today_theme_structure_text(drivers, pulse).split()))
    top_industry_text = " ".join(structure_labels)
    headline_blob = " ".join(str(item.get("title", "") or "") for item in items).lower()
    tech_structure_hits = _theme_keyword_hits(
        structure_labels,
        ("半导体", "消费电子", "通信", "通信设备", "光模块", "液冷", "存储", "PCB", "算力", "光学光电", "其他电子"),
    )
    power_structure_hits = _theme_keyword_hits(
        structure_labels,
        ("电网", "电力", "公用事业", "特高压", "储能", "逆变器"),
    )
    policy_structure_hits = _theme_keyword_hits(
        structure_labels,
        ("电网", "基建", "工程", "建材", "中字头"),
    )
    broad_structure_hits = _theme_keyword_hits(
        structure_labels,
        ("银行", "券商", "保险", "非银", "白酒", "家电", "证券"),
    )
    innovation_structure_hits = _theme_keyword_hits(
        structure_labels,
        ("创新药", "医药", "生物医药", "化学制药", "中药", "医疗器械", "CRO", "CXO"),
    )
    ai_headline_hits = _theme_headline_hits(
        items,
        ("半导体", "芯片", "光模块", "光通信", "cpo", "通信设备", "算力", "液冷", "存储", "pcb"),
    )
    power_headline_hits = _theme_headline_hits(
        items,
        ("电网", "电力", "公用事业", "特高压", "配电网", "变压器", "虚拟电厂"),
    )
    innovation_headline_hits = _theme_headline_hits(
        items,
        ("创新药", "新药", "药监局", "fda", "asco", "esmo", "临床", "license-out", "授权", "首付款", "里程碑"),
    )
    watch_boosts = _today_theme_watch_boosts(watchlist, watchlist_returns)
    scores = {
        "energy_shock": 0,
        "rate_growth": 0,
        "ai_semis": 0,
        "power_utilities": 0,
        "china_policy": 0,
        "broad_market_repair": 0,
        "innovation_medicine": 0,
    }
    if counter["energy"] + counter["geopolitics"] >= 2 and (brent_5d >= 0.12 or vix >= 25):
        return {"code": "energy_shock", "label": "能源冲击 + 地缘风险"}

    scores["rate_growth"] += counter["fed"] * 2
    if counter["fed"] >= 1 and vix < 22:
        scores["rate_growth"] += 1

    scores["ai_semis"] += counter["ai"] * 2 + counter["semiconductor"] * 2
    scores["ai_semis"] += _headline_hit_score(ai_headline_hits)
    if tech_structure_hits >= 2:
        scores["ai_semis"] += 3
    elif tech_structure_hits == 1:
        scores["ai_semis"] += 1
    if any(keyword in headline_blob for keyword in ("半导体", "芯片", "光模块", "液冷", "存储", "pcb", "硬科技")):
        scores["ai_semis"] += 2

    scores["power_utilities"] += counter["china_macro"]
    scores["power_utilities"] += _headline_hit_score(power_headline_hits)
    if power_structure_hits >= 2:
        scores["power_utilities"] += 3
    elif power_structure_hits == 1 and counter["china_macro"] >= 1:
        scores["power_utilities"] += 1

    scores["china_policy"] += counter["china_macro"] * 2 + counter["china_macro_domestic"] * 2
    if policy_structure_hits >= 2:
        scores["china_policy"] += 2
    elif policy_structure_hits == 1 and counter["china_macro"] + counter["china_macro_domestic"] >= 1:
        scores["china_policy"] += 1

    scores["broad_market_repair"] += counter["fed"] + counter["china_macro"] + counter["china_macro_domestic"]
    if broad_structure_hits >= 2:
        scores["broad_market_repair"] += 3
    elif broad_structure_hits == 1:
        scores["broad_market_repair"] += 1
    if any(keyword in headline_blob for keyword in ("放量大涨", "修复", "反转", "普涨", "风险偏好回暖")):
        scores["broad_market_repair"] += 2

    scores["innovation_medicine"] += counter["biotech"] * 2 + counter["pharma"] * 2 + counter["healthcare"] * 2
    scores["innovation_medicine"] += _headline_hit_score(innovation_headline_hits)
    if innovation_structure_hits >= 2:
        scores["innovation_medicine"] += 3
    elif innovation_structure_hits == 1:
        scores["innovation_medicine"] += 1
    if any(keyword in headline_blob for keyword in ("创新药", "医药", "biotech", "pharma", "license-out", "bd", "临床", "药企", "授权")):
        scores["innovation_medicine"] += 2

    for theme_name, boost in watch_boosts.items():
        scores[theme_name] += int(boost)

    theme = max(scores, key=scores.get)
    if scores[theme] < 4:
        return {
            "code": "macro_background",
            "label": "背景宏观主导",
            "secondary_codes": [],
            "secondary_labels": [],
        }
    secondary_threshold = max(5, scores[theme] - 3)
    secondary_codes = [
        name
        for name, _score in sorted(
            ((name, score) for name, score in scores.items() if name != theme and score >= secondary_threshold),
            key=lambda row: (row[1], row[0]),
            reverse=True,
        )[:2]
    ]
    if theme in DAY_THEME_LABELS:
        return {
            "code": theme,
            "label": DAY_THEME_LABELS[theme],
            "secondary_codes": secondary_codes,
            "secondary_labels": [DAY_THEME_LABELS[name] for name in secondary_codes if name in DAY_THEME_LABELS],
        }
    return {"code": "macro_background", "label": "背景宏观主导"}


def build_market_context(
    config: Mapping[str, Any],
    preferred_sources: Optional[Sequence[str]] = None,
    relevant_asset_types: Optional[Sequence[str]] = None,
) -> Dict[str, Any]:
    market_context_cfg = dict(dict(config or {}).get("market_context") or {})
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
    selected_asset_types = {str(item) for item in (relevant_asset_types or []) if str(item)}

    def _load_china_macro() -> tuple[Dict[str, Any], Optional[str]]:
        try:
            return load_china_macro_snapshot(dict(config)), None
        except Exception as exc:
            return {}, _client_safe_issue("中国宏观数据缺失", exc)

    def _load_global_proxy() -> tuple[Dict[str, Any], Optional[str]]:
        if market_context_cfg.get("skip_global_proxy"):
            return {}, "全球代理数据已按运行配置关闭，本次先按国内宏观与本地行情上下文生成。"
        if not global_proxy_runtime_enabled(config):
            return {}, "全球代理数据默认关闭，本次先按国内宏观与本地行情上下文生成。"
        try:
            with redirect_stderr(io.StringIO()):
                return load_global_proxy_snapshot(config), None
        except Exception as exc:
            return {}, _client_safe_issue("全球代理数据缺失", exc)
        finally:
            close_yfinance_runtime_caches()

    def _load_monitor_rows() -> tuple[List[Dict[str, Any]], Optional[str]]:
        if market_context_cfg.get("skip_market_monitor"):
            return [], "宏观资产监控已按运行配置关闭，本次不强行刷新跨市场监控快照。"
        if not (
            market_context_cfg.get("enable_market_monitor_runtime", False)
            or config.get("enable_market_monitor_runtime", False)
        ):
            return [], "宏观资产监控默认关闭，本次不强行刷新跨市场监控快照。"
        try:
            return MarketMonitorCollector(config).collect(), None
        except Exception as exc:
            return [], _client_safe_issue("宏观监控数据缺失", exc)
        finally:
            close_yfinance_runtime_caches()

    def _load_events() -> tuple[List[Dict[str, Any]], Optional[str]]:
        try:
            return EventsCollector(config).collect(mode="daily"), None
        except Exception as exc:
            return [], _client_safe_issue("事件日历缺失", exc)

    def _load_drivers() -> tuple[Dict[str, Any], Optional[str]]:
        if market_context_cfg.get("skip_market_drivers"):
            return {}, "板块驱动数据已按运行配置关闭，本次不强行刷新板块轮动与资金驱动快照。"
        try:
            return MarketDriversCollector(config).collect(), None
        except Exception as exc:
            return {}, _client_safe_issue("板块驱动数据缺失", exc)

    def _load_pulse() -> tuple[Dict[str, Any], Optional[str]]:
        try:
            return MarketPulseCollector(config).collect(), None
        except Exception as exc:
            return {}, _client_safe_issue("盘面情绪数据缺失", exc)

    def _load_watchlist_returns() -> Dict[str, pd.Series]:
        filtered = [
            item
            for item in watchlist
            if not selected_asset_types or str(item.get("asset_type", "cn_etf")) in selected_asset_types
        ]
        if not filtered:
            return {}

        def _fetch_watch_item(item: Mapping[str, Any]) -> tuple[str, pd.Series] | None:
            try:
                returns = _history_returns(fetch_asset_history(item["symbol"], item["asset_type"], dict(config)))
            except Exception:
                return None
            finally:
                close_yfinance_runtime_caches()
            return str(item["symbol"]), returns

        results: Dict[str, pd.Series] = {}
        with ThreadPoolExecutor(max_workers=min(4, len(filtered))) as pool:
            for payload in pool.map(_fetch_watch_item, filtered):
                if payload is None:
                    continue
                symbol, returns = payload
                results[symbol] = returns
        return results

    def _load_benchmark_returns() -> Dict[str, pd.Series]:
        filtered = [
            (asset_type, symbol, bench_asset_type)
            for asset_type, (symbol, bench_asset_type, _name) in BENCHMARKS.items()
            if not selected_asset_types or asset_type in selected_asset_types
        ]
        if not filtered:
            return {}

        def _fetch_benchmark(item: tuple[str, str, str]) -> tuple[str, pd.Series] | None:
            try:
                asset_type, symbol, bench_asset_type = item
                history = _safe_history(symbol, bench_asset_type, config)
                if history is None:
                    return None
                return asset_type, history["close"].pct_change().dropna()
            finally:
                close_yfinance_runtime_caches()

        results: Dict[str, pd.Series] = {}
        with ThreadPoolExecutor(max_workers=min(4, len(filtered))) as pool:
            for payload in pool.map(_fetch_benchmark, filtered):
                if payload is None:
                    continue
                asset_type, returns = payload
                results[asset_type] = returns
        return results

    with ThreadPoolExecutor(max_workers=7) as pool:
        china_macro_future = pool.submit(_load_china_macro)
        global_proxy_future = pool.submit(_load_global_proxy)
        monitor_rows_future = pool.submit(_load_monitor_rows)
        events_future = pool.submit(_load_events)
        drivers_future = pool.submit(_load_drivers)
        pulse_future = pool.submit(_load_pulse)
        watchlist_returns_future = pool.submit(_load_watchlist_returns)
        benchmark_returns_future = pool.submit(_load_benchmark_returns)

        china_macro, china_macro_note = china_macro_future.result()
        global_proxy, global_proxy_note = global_proxy_future.result()
        monitor_rows, monitor_rows_note = monitor_rows_future.result()
        if china_macro_note:
            notes.append(china_macro_note)
        if global_proxy_note:
            notes.append(global_proxy_note)
        if monitor_rows_note:
            notes.append(monitor_rows_note)
    try:
        regime_inputs = derive_regime_inputs(china_macro, global_proxy, monitor_rows)
        regime = RegimeDetector(regime_inputs).detect_regime()
    except Exception as exc:
        notes.append(_client_safe_issue("市场环境判断降级", exc))
    try:
        news_report = NewsCollector(config).collect(
            snapshots=watchlist,
            china_macro=china_macro,
            global_proxy=global_proxy,
            preferred_sources=preferred_sources or (),
            limit=20,
        )
    except Exception as exc:
        notes.append(_client_safe_issue("新闻源缺失", exc))
    events, events_note = events_future.result()
    drivers, drivers_note = drivers_future.result()
    pulse, pulse_note = pulse_future.result()
    if events_note:
        notes.append(events_note)
    if drivers_note:
        notes.append(drivers_note)
    if pulse_note:
        notes.append(pulse_note)
    watchlist_returns = watchlist_returns_future.result()
    benchmark_returns = benchmark_returns_future.result()
    close_yfinance_runtime_caches()

    hint_lines = _market_context_hint_lines(
        drivers,
        pulse,
        watchlist=watchlist,
        watchlist_returns=watchlist_returns,
    )
    hint_queries = [str(item).strip() for item in hint_lines[:6] if str(item).strip()]
    if hint_queries:
        backfill_timeout_seconds = float(dict(config or {}).get("market_context_intel_backfill_timeout_seconds", 6) or 6)
        try:
            backfilled_report = _timed_runtime_loader(
                lambda: _merge_market_context_intel_reports(
                    [
                        dict(
                            collect_intel_news_report(
                                query,
                                config=config,
                                limit=4,
                                recent_days=7,
                                structured_only=True,
                                note_prefix="共享 intel 回填",
                            )
                            or {}
                        )
                        for query in hint_queries
                    ],
                    limit=6,
                ),
                timeout_seconds=backfill_timeout_seconds,
                fallback=dict(news_report),
            )
            if dict(backfilled_report or {}).get("items") or dict(backfilled_report or {}).get("all_items"):
                news_report = dict(backfilled_report)
                if not str(news_report.get("fallback", "")).strip():
                    news_report["fallback"] = "intel_shared_upstream"
        except Exception as exc:
            notes.append(_client_safe_issue("共享 intel 回填失败", exc))

    day_theme = _today_theme(
        news_report,
        monitor_rows,
        drivers=drivers,
        pulse=pulse,
        watchlist=watchlist,
        watchlist_returns=watchlist_returns,
    )
    global_flow = _build_global_flow_report(watchlist, watchlist_returns, config)

    return {
        "as_of": datetime.now(),
        "china_macro": china_macro,
        "global_proxy": global_proxy,
        "global_flow": global_flow,
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
        "runtime_caches": {},
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


def _runtime_cache_bucket(context: Mapping[str, Any], bucket: str) -> Dict[Any, Any]:
    if not isinstance(context, dict):
        return {}
    caches = context.get("runtime_caches")
    if not isinstance(caches, dict):
        caches = {}
        context["runtime_caches"] = caches
    bucket_cache = caches.get(bucket)
    if not isinstance(bucket_cache, dict):
        bucket_cache = {}
        caches[bucket] = bucket_cache
    return bucket_cache


def _window_return(returns: Any, window: int) -> float:
    if not isinstance(returns, pd.Series):
        return 0.0
    cleaned = returns.dropna()
    if cleaned.empty:
        return 0.0
    tail = cleaned.tail(max(int(window), 1))
    if tail.empty:
        return 0.0
    return float((1.0 + tail).prod() - 1.0)


def _proxy_region(item: Mapping[str, Any]) -> str:
    region = str(item.get("region", "")).strip().upper()
    if region:
        return region
    asset_type = str(item.get("asset_type", "")).strip()
    if asset_type.startswith("cn") or asset_type == "futures":
        return "CN"
    if asset_type in {"hk", "hk_index"}:
        return "HK"
    if asset_type in {"us"}:
        return "US"
    symbol = str(item.get("symbol", "")).strip().upper()
    if symbol.endswith(".HK"):
        return "HK"
    if re.fullmatch(r"[A-Z]{1,5}", symbol):
        return "US"
    return ""


def _build_global_flow_report(
    watchlist: Sequence[Mapping[str, Any]],
    watchlist_returns: Mapping[str, pd.Series],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    for item in watchlist:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol:
            continue
        returns = watchlist_returns.get(symbol)
        if not isinstance(returns, pd.Series) or returns.dropna().empty:
            continue
        sector, _ = _normalize_sector(str(item.get("sector", "") or item.get("name", "")))
        rows.append(
            {
                "symbol": symbol,
                "sector": sector,
                "region": _proxy_region(item),
                "return_5d": _window_return(returns, 5),
                "return_20d": _window_return(returns, 20),
            }
        )
    return GlobalFlowCollector(config).collect(rows)


def _context_global_flow(context: Mapping[str, Any], config: Mapping[str, Any]) -> Dict[str, Any]:
    cached = dict(context.get("global_flow") or {})
    if cached:
        return cached
    bucket = _runtime_cache_bucket(context, "global_flow")
    if isinstance(bucket.get("report"), dict):
        return dict(bucket["report"])
    report = _build_global_flow_report(
        list(context.get("watchlist") or []),
        dict(context.get("watchlist_returns") or {}),
        config,
    )
    bucket["report"] = report
    if isinstance(context, dict):
        context["global_flow"] = report
    return dict(report)


def _social_trend_label(metrics: Mapping[str, Any], technical: Mapping[str, Any]) -> str:
    ma_signal = str(dict(technical.get("ma_system") or {}).get("signal", "")).strip()
    macd_signal = str(dict(technical.get("macd") or {}).get("signal", "")).strip()
    return_20d = float(metrics.get("return_20d", 0.0) or 0.0)
    return_5d = float(metrics.get("return_5d", 0.0) or 0.0)
    if ma_signal == "bullish" and macd_signal != "bearish" and return_20d >= -0.02:
        return "多头"
    if ma_signal == "bearish" and return_20d <= 0 and return_5d <= 0.01:
        return "空头"
    return "震荡"


def _analysis_social_sentiment(
    symbol: str,
    metrics: Mapping[str, Any],
    technical: Mapping[str, Any],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    volume_block = dict(technical.get("volume") or {})
    snapshot = {
        "return_1d": float(metrics.get("return_1d", 0.0) or 0.0),
        "return_5d": float(metrics.get("return_5d", 0.0) or 0.0),
        "return_20d": float(metrics.get("return_20d", 0.0) or 0.0),
        "volume_ratio": float(volume_block.get("vol_ratio", technical.get("volume_ratio", 1.0)) or 1.0),
        "trend": _social_trend_label(metrics, technical),
    }
    return SocialSentimentCollector(config).collect(symbol, snapshot)


def _analysis_proxy_signals(
    *,
    symbol: str,
    metrics: Mapping[str, Any],
    technical: Mapping[str, Any],
    runtime_context: Mapping[str, Any],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    market_flow = _context_global_flow(runtime_context, config)
    social_payload = _analysis_social_sentiment(symbol, metrics, technical, config)
    social_aggregate = dict(social_payload.get("aggregate") or {})
    market_flow_lines = list(market_flow.get("lines") or [])
    market_flow_line = str(market_flow_lines[0]).strip() if market_flow_lines else "当前没有形成稳定的市场风格代理结论。"
    social_line = str(social_aggregate.get("interpretation", "")).strip() or "当前没有形成稳定的个体情绪代理结论。"
    rows = [
        {
            "label": "市场风格代理",
            "interpretation": market_flow_line,
            "confidence_label": str(market_flow.get("confidence_label", "低")).strip() or "低",
            "confidence_score": market_flow.get("confidence_score"),
            "coverage_summary": str(market_flow.get("coverage_summary", "无有效代理样本")).strip() or "无有效代理样本",
            "limitation": str(next(iter(market_flow.get("limitations") or []), "")).strip(),
            "downgrade_impact": str(market_flow.get("downgrade_impact", "")).strip(),
            "method": str(market_flow.get("method", "proxy")).strip() or "proxy",
        },
        {
            "label": "情绪代理",
            "interpretation": social_line,
            "confidence_label": str(social_aggregate.get("confidence_label", "低")).strip() or "低",
            "confidence_score": social_aggregate.get("confidence_score"),
            "coverage_summary": "日涨跌 / 5日涨跌 / 20日涨跌 / 量能比 / 趋势",
            "limitation": str(next(iter(social_aggregate.get("limitations") or []), "")).strip(),
            "downgrade_impact": str(social_aggregate.get("downgrade_impact", "")).strip(),
            "method": str(social_aggregate.get("method", "proxy")).strip() or "proxy",
        },
    ]
    summary_lines = [
        f"市场风格代理：{market_flow_line}（置信度 `{rows[0]['confidence_label']}`，覆盖 `{rows[0]['coverage_summary']}`）。",
        f"情绪代理：{social_line}（置信度 `{rows[1]['confidence_label']}`）。",
    ]
    limitations = [
        str(item).strip()
        for item in (rows[0]["limitation"], rows[1]["limitation"])
        if str(item).strip()
    ]
    downgrade_lines = [
        str(item).strip()
        for item in (rows[0]["downgrade_impact"], rows[1]["downgrade_impact"])
        if str(item).strip()
    ]
    return {
        "market_flow": market_flow,
        "social_sentiment": social_payload,
        "rows": rows,
        "summary_lines": summary_lines,
        "limitations": limitations,
        "downgrade_lines": downgrade_lines,
    }


def summarize_proxy_contracts(
    *,
    market_proxy: Optional[Mapping[str, Any]] = None,
    social_payloads: Optional[Sequence[Mapping[str, Any]]] = None,
    total: Optional[int] = None,
) -> Dict[str, Any]:
    market_flow = dict(market_proxy or {})
    social_counter: Counter[str] = Counter()
    social_limitations: List[str] = []
    social_downgrade = ""
    covered = 0
    rows = list(social_payloads or [])
    for item in rows:
        aggregate = dict(item.get("aggregate") or item or {})
        if not aggregate:
            continue
        covered += 1
        label = str(aggregate.get("confidence_label", "")).strip()
        if label:
            social_counter[label] += 1
        limitation = str(next(iter(aggregate.get("limitations") or []), "")).strip()
        if limitation and limitation not in social_limitations:
            social_limitations.append(limitation)
        if not social_downgrade:
            social_downgrade = str(aggregate.get("downgrade_impact", "")).strip()
    return {
        "market_flow": {
            "interpretation": str(next(iter(market_flow.get("lines") or []), "当前没有形成稳定的市场风格代理结论。")).strip(),
            "confidence_label": str(market_flow.get("confidence_label", "低")).strip() or "低",
            "confidence_score": market_flow.get("confidence_score"),
            "coverage_summary": str(market_flow.get("coverage_summary", "无有效代理样本")).strip() or "无有效代理样本",
            "limitation": str(next(iter(market_flow.get("limitations") or []), "")).strip(),
            "downgrade_impact": str(market_flow.get("downgrade_impact", "")).strip(),
        },
        "social_sentiment": {
            "covered": covered,
            "total": int(total if total is not None else len(rows)),
            "confidence_labels": dict(sorted(social_counter.items())),
            "coverage_summary": (
                f"{covered}/{int(total if total is not None else len(rows))} 只候选已生成情绪代理"
                if (rows or total is not None)
                else "0/0 只候选已生成情绪代理"
            ),
            "limitation": social_limitations[0] if social_limitations else "",
            "downgrade_impact": social_downgrade,
        },
        "lines": [
            (
                "市场风格代理："
                + str(next(iter(market_flow.get("lines") or []), "当前没有形成稳定的市场风格代理结论。")).strip()
                + f"（置信度 `{str(market_flow.get('confidence_label', '低')).strip() or '低'}`）。"
            ),
            (
                f"情绪代理覆盖 `{covered}/{int(total if total is not None else len(rows))}` 只候选；"
                f"置信度分布 {dict(sorted(social_counter.items())) or {'低': 0}}。"
            ),
        ],
    }


def summarize_proxy_contracts_from_analyses(
    analyses: Sequence[Mapping[str, Any]],
    *,
    market_proxy: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    rows = list(analyses or [])
    market_flow = dict(market_proxy or {})
    if not market_flow:
        market_flow = dict(dict((rows[0].get("proxy_signals") if rows else {}) or {}).get("market_flow") or {})
    social_payloads = [dict(dict(item.get("proxy_signals") or {}).get("social_sentiment") or {}) for item in rows]
    return summarize_proxy_contracts(
        market_proxy=market_flow,
        social_payloads=social_payloads,
        total=len(rows),
    )


def _context_drivers(context: Mapping[str, Any], config: Mapping[str, Any]) -> Dict[str, Any]:
    if "drivers" in context:
        return dict(context.get("drivers") or {})
    cache = _runtime_cache_bucket(context, "drivers")
    if "value" not in cache:
        try:
            cache["value"] = MarketDriversCollector(config).collect()
        except Exception:
            cache["value"] = {}
    return dict(cache.get("value") or {})


def _context_industry_index_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    asset_type = str(metadata.get("asset_type", "")).strip()
    if asset_type == "cn_stock" and _runtime_feature_disabled(context, "stock_pool_skip_industry_lookup_runtime"):
        return {
            "status": "skipped",
            "diagnosis": "runtime_skip",
            "items": [],
            "fallback": "runtime_skip",
            "is_fresh": False,
            "disclosure": "个股标准行业/指数框架逐票补查已按快路径跳过，本轮先使用候选池已有行业画像。",
        }
    cache = _runtime_cache_bucket(context, "industry_index_snapshot")
    key = (
        asset_type,
        str(metadata.get("symbol", "")).strip(),
        str(metadata.get("index_code", "")).strip(),
        str(metadata.get("index_name", "")).strip(),
        str(metadata.get("benchmark", "")).strip(),
    )
    if key not in cache:
        collector = IndustryIndexCollector(dict(context.get("config", {})))
        try:
            if asset_type == "cn_stock":
                cache[key] = collector.get_stock_industry_snapshot(
                    str(metadata.get("symbol", "")).strip(),
                    reference_date=_context_now(context),
                )
            elif asset_type == "cn_etf":
                cache[key] = collector.get_etf_industry_snapshot(
                    metadata,
                    fund_profile=fund_profile,
                    reference_date=_context_now(context),
                )
            else:
                cache[key] = {}
        except Exception as exc:
            cache[key] = {
                "status": "blocked",
                "diagnosis": "fetch_error",
                "items": [],
                "fallback": "none",
                "is_fresh": False,
                "disclosure": f"标准行业/指数框架当前不可用，本轮按缺失处理：{exc}",
            }
    return dict(cache.get(key) or {})


def _index_topic_code_candidates(
    metadata: Mapping[str, Any],
    *,
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    overview = dict((fund_profile or {}).get("overview") or {})
    etf_snapshot = dict((fund_profile or {}).get("etf_snapshot") or {})
    candidates = [
        metadata.get("index_code"),
        metadata.get("benchmark_symbol"),
        metadata.get("benchmark_code"),
        etf_snapshot.get("index_code"),
        overview.get("ETF基准指数代码"),
    ]
    if str(metadata.get("asset_type", "")).strip() == "cn_index":
        candidates.append(metadata.get("symbol"))
    return _unique_strings([str(item).strip() for item in candidates if str(item or "").strip()])


def _index_topic_anchor_keywords(
    metadata: Mapping[str, Any],
    *,
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    overview = dict((fund_profile or {}).get("overview") or {})
    etf_snapshot = dict((fund_profile or {}).get("etf_snapshot") or {})
    anchors = [
        overview.get("跟踪标的"),
        overview.get("业绩比较基准"),
        overview.get("ETF基准指数中文全称"),
        etf_snapshot.get("index_name"),
        metadata.get("benchmark_name"),
        metadata.get("benchmark"),
        metadata.get("index_name"),
    ]
    return _unique_strings([str(item).strip() for item in anchors if str(item or "").strip()])


def _index_topic_keyword_specificity(value: Any) -> int:
    normalized = re.sub(r"[\s指数收益率价格主题（）()·*/+_-]+", "", str(value or "").strip().lower())
    return len(normalized)


def _asset_uses_index_topic_bundle(
    metadata: Mapping[str, Any],
    *,
    fund_profile: Optional[Mapping[str, Any]] = None,
    asset_type: str = "",
) -> bool:
    payload: Dict[str, Any] = dict(metadata or {})
    if asset_type and not str(payload.get("asset_type", "")).strip():
        payload["asset_type"] = str(asset_type).strip()
    if fund_profile:
        payload["fund_profile"] = dict(fund_profile or {})
    return uses_index_mainline(payload)


def _context_index_topic_bundle(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    *,
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    asset_type = str(metadata.get("asset_type", "")).strip()
    if asset_type not in {"cn_stock", "cn_etf", "cn_fund", "cn_index"}:
        return {}
    if asset_type == "cn_stock":
        return {
            "index_snapshot": {},
            "technical_snapshot": {
                "status": "skipped",
                "diagnosis": "not_applicable",
                "disclosure": "个股主链默认按板块/主题/行业行情理解，不再把指数专题补充当成正文主路径。",
            },
            "history_snapshots": {},
            "constituent_weights": pd.DataFrame(),
            "fallback": "not_applicable",
            "is_fresh": False,
        }
    if asset_type == "cn_fund" and not _asset_uses_index_topic_bundle(metadata, fund_profile=fund_profile, asset_type=asset_type):
        return {
            "index_snapshot": {},
            "technical_snapshot": {
                "status": "skipped",
                "diagnosis": "not_applicable",
                "disclosure": "主动基金主链默认按基金经理、持仓和风格暴露理解，不再把指数专题补充当成正文主路径。",
            },
            "history_snapshots": {},
            "constituent_weights": pd.DataFrame(),
            "fallback": "not_applicable",
            "is_fresh": False,
        }
    if _runtime_feature_disabled(context, "skip_index_topic_bundle_runtime"):
        return {
            "index_snapshot": {},
            "technical_snapshot": {
                "status": "skipped",
                "diagnosis": "runtime_skip",
                "disclosure": "指数专题主链在 discovery 预筛阶段已跳过；如进入正式候选，会补完整指数/主题结构。",
            },
            "history_snapshots": {},
            "constituent_weights": pd.DataFrame(),
            "fallback": "runtime_skip",
            "is_fresh": False,
        }
    code_candidates = _index_topic_code_candidates(metadata, fund_profile=fund_profile)
    explicit_code = next((item for item in code_candidates if item), "")
    anchor_keywords = _index_topic_anchor_keywords(metadata, fund_profile=fund_profile)
    keywords = _valuation_keywords(metadata, asset_type, fund_profile)
    if explicit_code and anchor_keywords:
        keywords = _unique_strings(
            [
                *anchor_keywords,
                *[
                    item
                    for item in keywords
                    if item not in anchor_keywords and _index_topic_keyword_specificity(item) >= 4
                ][:4],
            ]
        )
    cache = _runtime_cache_bucket(context, "index_topic_bundle")
    key = (
        asset_type,
        str(metadata.get("symbol", "")).strip(),
        explicit_code,
        tuple(keywords[:8]),
    )
    if key not in cache:
        collector = IndexTopicCollector(dict(context.get("config", {})))
        timeout_seconds = float(dict(context.get("config") or {}).get("index_topic_bundle_timeout_seconds", 0) or 0)
        timeout_fallback = {
            "index_snapshot": {},
            "technical_snapshot": {
                "status": "blocked",
                "diagnosis": "timeout",
                "disclosure": f"指数专题主链拉取超时（>{int(timeout_seconds)}s），本轮按缺失处理，不把它写成已确认趋势。"
                if timeout_seconds > 0
                else "指数专题主链当前不可用，本轮按缺失处理。",
            },
            "history_snapshots": {},
            "constituent_weights": pd.DataFrame(),
            "fallback": "timeout" if timeout_seconds > 0 else "none",
            "is_fresh": False,
        }
        try:
            cache[key] = _timed_runtime_loader(
                lambda: collector.get_index_bundle(
                    index_code=explicit_code,
                    keywords=keywords,
                    top_n=10,
                    reference_date=_context_now(context),
                ),
                timeout_seconds=timeout_seconds,
                fallback=timeout_fallback,
            )
        except Exception as exc:
            cache[key] = {
                "index_snapshot": {},
                "technical_snapshot": {
                    "status": "blocked",
                    "diagnosis": "fetch_error",
                    "disclosure": f"指数专题主链当前不可用，本轮按缺失处理：{exc}",
                },
                "constituent_weights": pd.DataFrame(),
                "fallback": "none",
                "is_fresh": False,
            }
    return dict(cache.get(key) or {})


def _timed_runtime_loader(loader, *, timeout_seconds: float, fallback: Any) -> Any:
    timeout = float(timeout_seconds or 0)
    if timeout <= 0:
        return loader()
    state: Dict[str, Any] = {"value": fallback, "error": None}

    def _run() -> None:
        try:
            state["value"] = loader()
        except BaseException as exc:  # pragma: no cover - surfaced after join
            state["error"] = exc

    worker = threading.Thread(target=_run, daemon=True)
    worker.start()
    worker.join(timeout)
    if worker.is_alive():
        return fallback
    if state["error"] is not None:
        raise state["error"]
    return state["value"]


def _enrich_metadata_with_index_topic_bundle(
    metadata: Mapping[str, Any],
    bundle: Mapping[str, Any],
) -> Dict[str, Any]:
    enriched = dict(metadata)
    snapshot = dict(bundle.get("index_snapshot") or {})
    technical = dict(bundle.get("technical_snapshot") or {})
    history_snapshots = dict(bundle.get("history_snapshots") or {})
    weights = bundle.get("constituent_weights")
    if snapshot:
        index_name = str(snapshot.get("index_name", "")).strip()
        index_code = str(snapshot.get("index_code", "")).strip()
        if index_name:
            enriched.setdefault("benchmark", index_name)
            enriched.setdefault("benchmark_name", index_name)
            enriched["index_framework_label"] = index_name
        if index_code:
            enriched.setdefault("benchmark_symbol", index_code)
            enriched.setdefault("index_code", index_code)
    if technical:
        enriched["index_technical_snapshot"] = technical
    if history_snapshots:
        enriched["index_history_snapshots"] = history_snapshots
    if isinstance(weights, pd.DataFrame) and not weights.empty:
        top_weight = pd.to_numeric(weights.get("weight", pd.Series(dtype=float)), errors="coerce").fillna(0.0)
        if not top_weight.empty:
            enriched["index_top_weight_sum"] = float(top_weight.sum())
        first = weights.iloc[0]
        enriched["index_top_constituent_name"] = str(first.get("name", "")).strip()
        enriched["index_top_constituent_symbol"] = str(first.get("symbol", "")).strip()
    if bundle:
        enriched["index_topic_bundle"] = bundle
    return enriched


def _index_history_signal_contract(
    index_name: str,
    snapshot: Mapping[str, Any],
    *,
    period: str,
) -> Dict[str, Any]:
    cleaned_name = str(index_name or "").strip()
    cleaned_period = str(period or "").strip().lower()
    period_label = {"weekly": "周线", "monthly": "月线"}.get(cleaned_period, cleaned_period or "历史")
    source_label = f"指数{period_label}"
    signal_type = f"{period_label}结构"
    trend_label = str(snapshot.get("trend_label", "")).strip()
    momentum_label = str(snapshot.get("momentum_label", "")).strip()
    summary = str(snapshot.get("summary", "")).strip()
    strength = str(snapshot.get("signal_strength", "")).strip() or "中"
    if trend_label in {"趋势偏强", "修复中"}:
        prefix = f"偏利多，先看 `{cleaned_name}` 的{period_label}是否继续 {trend_label}。"
    elif trend_label == "趋势偏弱":
        prefix = f"偏谨慎，先看 `{cleaned_name}` 的{period_label}是否继续承压。"
    elif trend_label:
        prefix = f"先按 `{cleaned_name}` 的{period_label}{trend_label}理解，不把单日波动误判成趋势。"
    else:
        prefix = f"先按 `{cleaned_name}` 的{period_label}结构理解，不把单日波动误判成趋势。"
    if summary:
        conclusion = f"{prefix} {summary}".strip()
    else:
        conclusion = prefix
    if momentum_label and momentum_label != "动能中性":
        conclusion = f"{conclusion}（{momentum_label}）"
    return {
        "source_label": source_label,
        "signal_type": signal_type,
        "strength": strength,
        "conclusion": conclusion,
        "trend_label": trend_label,
        "momentum_label": momentum_label,
        "summary": summary,
    }


def refresh_etf_analysis_report_fields(
    analysis: Mapping[str, Any],
    *,
    config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    updated = dict(analysis or {})
    asset_type = str(updated.get("asset_type", "")).strip()
    fund_profile = dict(updated.get("fund_profile") or {})
    if asset_type not in {"cn_etf", "cn_fund"} or not fund_profile:
        return updated

    metadata = _enrich_metadata_with_fund_profile(dict(updated.get("metadata") or {}), fund_profile)
    context = {
        "config": dict(config or {}),
        "as_of": str(updated.get("generated_at", "")).strip()[:10] or datetime.now().strftime("%Y-%m-%d"),
        "drivers": {
            "industry_spot": pd.DataFrame(),
            "concept_spot": pd.DataFrame(),
            "hot_rank": pd.DataFrame(),
        },
        "runtime_caches": {},
        "fund_profile": fund_profile,
    }
    industry_snapshot = _context_industry_index_snapshot(metadata, context, fund_profile=fund_profile)
    if industry_snapshot:
        metadata = _enrich_metadata_with_industry_index_snapshot(metadata, industry_snapshot)
    index_topic_bundle = _context_index_topic_bundle(metadata, context, fund_profile=fund_profile)
    if index_topic_bundle:
        metadata = _enrich_metadata_with_index_topic_bundle(metadata, index_topic_bundle)
    updated["metadata"] = metadata

    benchmark_name = str(metadata.get("benchmark_name", "")).strip() or str(metadata.get("benchmark", "")).strip()
    benchmark_symbol = str(metadata.get("benchmark_symbol", "")).strip() or str(metadata.get("index_code", "")).strip()
    if benchmark_name:
        updated["benchmark_name"] = benchmark_name
    if benchmark_symbol:
        updated["benchmark_symbol"] = benchmark_symbol

    refreshed_rows = _market_event_rows_from_context(metadata, context, fund_profile)
    if refreshed_rows:
        updated["market_event_rows"] = refreshed_rows
    return updated


def _index_topic_rows_from_bundle(
    metadata: Mapping[str, Any],
    bundle: Mapping[str, Any],
    *,
    as_of: str,
) -> List[List[str]]:
    snapshot = dict(bundle.get("index_snapshot") or {})
    technical = dict(bundle.get("technical_snapshot") or {})
    weights = bundle.get("constituent_weights")
    if not snapshot and not technical:
        return []

    asset_type = str(metadata.get("asset_type", "")).strip()
    symbol = str(metadata.get("symbol", "")).strip()
    display_name = str(metadata.get("name", symbol)).strip() or symbol
    index_name = str(snapshot.get("index_name", "") or metadata.get("benchmark_name", "") or metadata.get("benchmark", "")).strip()
    if not index_name:
        return []

    pct_change = pd.to_numeric(pd.Series([technical.get("pct_change")]), errors="coerce").iloc[0]
    strength = str(technical.get("signal_strength", "")).strip() or ("中" if pd.isna(pct_change) else ("高" if float(pct_change) >= 3 else "中" if float(pct_change) >= 1 else "低"))
    rows: List[List[str]] = []

    summary_bits: List[str] = []
    pe_ttm = pd.to_numeric(pd.Series([snapshot.get("pe_ttm")]), errors="coerce").iloc[0]
    pb = pd.to_numeric(pd.Series([snapshot.get("pb")]), errors="coerce").iloc[0]
    if not pd.isna(pct_change):
        summary_bits.append(f"{float(pct_change):+.2f}%")
    if not pd.isna(pe_ttm):
        summary_bits.append(f"PE {float(pe_ttm):.1f}x")
    if not pd.isna(pb):
        summary_bits.append(f"PB {float(pb):.1f}x")

    if asset_type in {"cn_etf", "cn_fund"}:
        title = f"跟踪指数框架：{display_name} 跟踪 {index_name}"
        source_label = "跟踪指数/框架"
        signal_type = "标准指数框架"
    elif asset_type == "cn_index":
        title = f"指数主链：{index_name}"
        source_label = "指数主链"
        signal_type = "标准指数框架"
    else:
        title = f"相关指数框架：{index_name}"
        source_label = "相关指数/框架"
        signal_type = "行业/指数映射"
    if summary_bits:
        title += f"（{' / '.join(summary_bits)}）"
    trend_label = str(technical.get("trend_label", "")).strip()
    conclusion = (
        f"偏利多，先看 `{index_name}` 的标准指数框架是否继续支撑当前方向。"
        if trend_label in {"趋势偏强", "修复中"}
        else f"偏谨慎，先看 `{index_name}` 的指数主链是否继续承压。"
        if trend_label
        else f"先按 `{index_name}` 的标准指数框架理解，不把模糊板块词当主线。"
    )
    rows.append([as_of, title, source_label, strength, index_name, "", signal_type, conclusion])

    history_snapshots = dict(bundle.get("history_snapshots") or {})
    for period in ("monthly", "weekly"):
        history_snapshot = dict(history_snapshots.get(period) or {})
        if not history_snapshot or str(history_snapshot.get("status", "")).strip() != "matched":
            continue
        history_contract = _index_history_signal_contract(index_name, history_snapshot, period=period)
        rows.append(
            [
                as_of,
                f"指数{history_contract['source_label'].replace('指数', '')}：{index_name}"
                + (f" {history_contract['trend_label']}" if history_contract["trend_label"] else ""),
                history_contract["source_label"],
                history_contract["strength"],
                index_name,
                "",
                history_contract["signal_type"],
                history_contract["conclusion"],
            ]
        )

    if trend_label:
        momentum_label = str(technical.get("momentum_label", "")).strip()
        tech_detail = str(technical.get("detail", "")).strip()
        tech_title = f"指数技术面：{index_name} {trend_label}"
        if momentum_label:
            tech_title += f" / {momentum_label}"
        rows.append(
            [
                as_of,
                tech_title,
                "指数技术面",
                strength,
                index_name,
                "",
                "技术确认",
                (f"优先按 `{trend_label}` 理解跟踪指数的相对强弱和节奏。" + (f" {tech_detail}" if tech_detail else "")).strip(),
            ]
        )

    if isinstance(weights, pd.DataFrame) and not weights.empty:
        working = weights.copy()
        working["weight"] = pd.to_numeric(working.get("weight", pd.Series(dtype=float)), errors="coerce")
        working = working.dropna(subset=["weight"])
        matched = working[working.get("symbol", pd.Series("", index=working.index)).astype(str).str.strip() == symbol] if symbol else pd.DataFrame()
        if asset_type == "cn_stock" and not matched.empty:
            weight_value = float(matched.iloc[0]["weight"])
            rows.append(
                [
                    as_of,
                    f"指数权重位置：{display_name} 在 {index_name} 中权重约 {weight_value:.2f}%",
                    "指数成分/权重",
                    "中" if weight_value < 3 else "高",
                    index_name,
                    "",
                    "龙头权重暴露",
                    f"标准指数成分表显示 `{display_name}` 在 `{index_name}` 中具备明确权重位置，不再只靠关键词猜主线。",
                ]
            )
        elif asset_type in {"cn_etf", "cn_fund", "cn_index"}:
            top_concentration = float(working["weight"].sum()) if not working.empty else 0.0
            leaders = "、".join(
                f"{str(row.get('name', '')).strip() or str(row.get('symbol', '')).strip()} {float(row.get('weight', 0.0)):.1f}%"
                for _, row in working.head(3).iterrows()
            )
            rows.append(
                [
                    as_of,
                    f"指数成分权重：前十权重合计 {top_concentration:.1f}%"
                    + (f"；核心成分 {leaders}" if leaders else ""),
                    "指数成分/权重",
                    "中" if top_concentration < 40 else "高",
                    index_name,
                    "",
                    "成分权重结构",
                    f"指数主链已明确 `{index_name}` 的核心成分和权重结构，推荐理由优先按标准指数暴露理解。",
                ]
            )
    return rows


def _etf_profile_proxy_rows(
    metadata: Mapping[str, Any],
    fund_profile: Mapping[str, Any],
    *,
    as_of: str,
    has_index_weight_row: bool,
    has_standard_industry_row: bool,
) -> List[List[str]]:
    asset_type = str(metadata.get("asset_type", "")).strip()
    if asset_type not in {"cn_etf", "cn_fund"}:
        return []

    display_name = str(metadata.get("name", "")).strip() or str(metadata.get("symbol", "")).strip()
    rows: List[List[str]] = []
    etf_snapshot = dict((fund_profile or {}).get("etf_snapshot") or {})

    share_change = pd.to_numeric(pd.Series([etf_snapshot.get("etf_share_change")]), errors="coerce").dropna()
    share_change_pct = pd.to_numeric(pd.Series([etf_snapshot.get("etf_share_change_pct")]), errors="coerce").dropna()
    share_as_of = str(etf_snapshot.get("share_as_of", "")).strip()
    if not share_change.empty:
        share_value = float(share_change.iloc[0])
        pct_text = f" ({float(share_change_pct.iloc[0]):+.2f}%)" if not share_change_pct.empty else ""
        if share_value > 0:
            title = f"份额申赎确认：{display_name} 最近净创设 {share_value:+.2f} 亿份{pct_text}"
            signal_type = "份额净创设"
            strength = "高" if share_value >= 5 else "中"
            conclusion = "偏利多，ETF 份额扩张说明场外申购在配合当前主线，不只是价格抬升。"
        elif share_value < 0:
            title = f"份额申赎提示：{display_name} 最近净赎回 {share_value:+.2f} 亿份{pct_text}"
            signal_type = "份额净赎回"
            strength = "高" if share_value <= -5 else "中"
            conclusion = "偏谨慎，ETF 最近有净赎回，当前价格变化还没有完全得到份额流入确认。"
        else:
            title = f"份额申赎跟踪：{display_name} 最近份额基本持平"
            signal_type = "份额中性"
            strength = "低"
            conclusion = "中性，当前 ETF 份额没有明显扩张或赎回，先继续看主线和价格确认。"
        if share_as_of:
            title += f"（{share_as_of}）"
        rows.append(
            [
                as_of,
                title,
                "ETF份额规模",
                strength,
                display_name,
                "",
                signal_type,
                conclusion,
            ]
        )

    top_holdings = list((fund_profile or {}).get("top_holdings") or [])
    if (not has_index_weight_row) and top_holdings:
        holding_bits: List[str] = []
        for item in top_holdings[:3]:
            holding_name = str(item.get("股票名称", "")).strip() or str(item.get("股票代码", "")).strip()
            weight_value = pd.to_numeric(pd.Series([item.get("占净值比例")]), errors="coerce").iloc[0]
            if not holding_name:
                continue
            if pd.notna(weight_value):
                holding_bits.append(f"{holding_name} {float(weight_value):.1f}%")
            else:
                holding_bits.append(holding_name)
        if holding_bits:
            rows.append(
                [
                    as_of,
                    f"跟踪成分画像：{display_name} 最近披露持仓集中在 " + "、".join(holding_bits),
                    "ETF持仓代理",
                    "中",
                    display_name,
                    "",
                    "成分画像",
                    "当前未拿到可用 index_weight，先用 ETF 最近披露持仓做代理，不把它误写成实时指数权重。",
                ]
            )

    industry_rows = list((fund_profile or {}).get("industry_allocation") or [])
    if (not has_standard_industry_row) and industry_rows:
        exposure_bits: List[str] = []
        for item in industry_rows[:3]:
            industry_name = str(item.get("行业类别", "")).strip()
            ratio_value = pd.to_numeric(pd.Series([item.get("占净值比例")]), errors="coerce").iloc[0]
            if not industry_name or industry_name in {"综合"}:
                continue
            if pd.notna(ratio_value):
                exposure_bits.append(f"{industry_name} {float(ratio_value):.1f}%")
            else:
                exposure_bits.append(industry_name)
        if exposure_bits:
            rows.append(
                [
                    as_of,
                    f"行业暴露画像：{display_name} 最近披露主要暴露在 " + "、".join(exposure_bits),
                    "ETF行业代理",
                    "中",
                    display_name,
                    "",
                    "行业归属",
                    "当前未拿到可用申万/中信行业指数链，先用 ETF 最近披露行业分布做代理，不把它误写成实时行业扩散。",
                ]
            )

    return rows


def _preferred_industry_index_items(
    snapshot: Mapping[str, Any],
    *,
    limit: int = 1,
) -> List[Dict[str, Any]]:
    items = [dict(item) for item in list(snapshot.get("items") or []) if dict(item)]
    if not items:
        return []

    def _sort_key(item: Mapping[str, Any]) -> tuple[int, int, float]:
        family_rank = {"sw": 0, "ci": 1}.get(str(item.get("family", "")).strip(), 9)
        level_rank = {"L2": 0, "L3": 1, "L1": 2}.get(str(item.get("level", "")).strip(), 9)
        pct_value = pd.to_numeric(pd.Series([item.get("pct_change")]), errors="coerce").iloc[0]
        strength_rank = abs(float(pct_value)) if not pd.isna(pct_value) else -1.0
        return (family_rank, level_rank, -strength_rank)

    selected: List[Dict[str, Any]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in sorted(items, key=_sort_key):
        identity = (
            str(item.get("family", "")).strip(),
            str(item.get("index_code", "")).strip(),
            str(item.get("index_name", "")).strip(),
        )
        if identity in seen:
            continue
        seen.add(identity)
        selected.append(item)
        if len(selected) >= max(int(limit), 1):
            break
    return selected


def _industry_index_rows_from_snapshot(
    metadata: Mapping[str, Any],
    snapshot: Mapping[str, Any],
    *,
    as_of: str,
) -> List[List[str]]:
    if str(snapshot.get("status", "")).strip() != "matched":
        return []
    asset_type = str(metadata.get("asset_type", "")).strip()
    subject_name = str(metadata.get("name", "")).strip() or str(metadata.get("symbol", "")).strip()
    if not subject_name:
        return []

    limit = 1 if asset_type == "cn_stock" else 2 if asset_type == "cn_etf" else 1
    rows: List[List[str]] = []
    for item in _preferred_industry_index_items(snapshot, limit=limit):
        family_label = str(item.get("family_label", "")).strip() or ("申万" if str(item.get("family", "")).strip() == "sw" else "中信")
        framework_label = str(item.get("framework_source", "")).strip() or f"{family_label}行业"
        index_name = str(item.get("index_name", "")).strip()
        if not index_name:
            continue
        pct_value = pd.to_numeric(pd.Series([item.get("pct_change")]), errors="coerce").iloc[0]
        move_value = None if pd.isna(pct_value) else float(pct_value)
        move_text = f"（{move_value:+.2f}%）" if move_value is not None else ""
        strength = str(item.get("signal_strength", "")).strip() or "中"
        if asset_type == "cn_etf":
            signal_type = "行业/指数框架" if move_value is None or move_value >= 0 else "行业框架承压"
            conclusion = (
                f"偏利多，先按 `{framework_label}` 对应的 `{index_name}` 去理解它的相对强弱和行业扩散。"
                if move_value is None or move_value >= 0
                else f"偏谨慎，`{index_name}` 当前回落，先别把标准行业归属直接写成顺风催化。"
            )
            title = f"跟踪指数/行业框架：{subject_name} 对应 {framework_label}·{index_name}{move_text}"
        else:
            signal_type = "标准行业归因" if move_value is None or move_value >= 0 else "行业框架承压"
            conclusion = (
                f"偏利多，`{subject_name}` 先按 `{framework_label}` 的 `{index_name}` 去理解，而不是继续靠模糊板块词。"
                if move_value is None or move_value >= 0
                else f"偏谨慎，`{subject_name}` 虽属于 `{index_name}`，但对应标准行业指数当前本身在回落。"
            )
            title = f"标准行业框架：{subject_name} 属于 {framework_label}·{index_name}{move_text}"
        rows.append(
            [
                as_of,
                title,
                f"{family_label}行业框架",
                strength,
                index_name,
                "",
                signal_type,
                conclusion,
            ]
        )
    return rows


def _context_stock_news(symbol: str, context: Mapping[str, Any]) -> List[Mapping[str, Any]]:
    cleaned = str(symbol).strip()
    if not cleaned:
        return []
    if _runtime_feature_disabled(context, "skip_cn_stock_direct_news_runtime"):
        return []
    cache = _runtime_cache_bucket(context, "stock_news")
    if cleaned not in cache:
        try:
            cache[cleaned] = NewsCollector(dict(context.get("config", {}))).get_stock_news(cleaned)
        except Exception:
            cache[cleaned] = []
    return list(cache.get(cleaned) or [])


def _context_cn_index_snapshot(
    keywords: Sequence[str],
    context: Mapping[str, Any],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    cleaned_keywords = tuple(dict.fromkeys(str(item).strip() for item in keywords if str(item).strip()))
    if not cleaned_keywords:
        return {}
    cache = _runtime_cache_bucket(context, "cn_index_snapshot")
    if cleaned_keywords not in cache:
        try:
            cache[cleaned_keywords] = ValuationCollector(config).get_cn_index_snapshot(list(cleaned_keywords)) or {}
        except Exception:
            cache[cleaned_keywords] = {}
    return dict(cache.get(cleaned_keywords) or {})


def _context_cn_index_financial_proxies(
    index_code: str,
    *,
    top_n: int,
    context: Mapping[str, Any],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    cleaned = str(index_code).strip()
    if not cleaned:
        return {}
    cache = _runtime_cache_bucket(context, "cn_index_financial_proxies")
    key = (cleaned, int(top_n))
    if key not in cache:
        try:
            cache[key] = ValuationCollector(config).get_cn_index_financial_proxies(cleaned, top_n=top_n) or {}
        except Exception:
            cache[key] = {}
    return dict(cache.get(key) or {})


def _context_cn_index_proxy_candidates(
    keywords: Sequence[str],
    *,
    context: Mapping[str, Any],
    config: Mapping[str, Any],
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    seen_codes: set[str] = set()

    def _append(snapshot: Optional[Mapping[str, Any]]) -> None:
        if not snapshot:
            return
        code = str(snapshot.get("index_code", "")).strip()
        if not code or code in seen_codes:
            return
        seen_codes.add(code)
        candidates.append(dict(snapshot))

    cleaned_keywords = [str(item).strip() for item in keywords if str(item).strip()]
    _append(_context_cn_index_snapshot(cleaned_keywords, context, config))
    if candidates:
        return candidates
    for keyword in cleaned_keywords:
        _append(_context_cn_index_snapshot([keyword], context, config))
    return candidates


def _concentration_proxy_from_index_topic_bundle(
    bundle: Mapping[str, Any],
    *,
    symbol: str = "",
) -> Dict[str, Any]:
    weights = bundle.get("constituent_weights")
    if not isinstance(weights, pd.DataFrame) or weights.empty:
        return {}
    working = weights.copy()
    working["weight"] = pd.to_numeric(working.get("weight", pd.Series(dtype=float)), errors="coerce")
    working = working.dropna(subset=["weight"])
    if working.empty:
        return {}

    top5 = working.head(5).copy()
    top_concentration = float(top5["weight"].sum())
    if top_concentration <= 0:
        return {}

    cleaned_symbol = str(symbol).strip()
    constituents = top5.to_dict("records")
    matched_current_symbol = False
    if cleaned_symbol:
        matched_current_symbol = any(str(item.get("symbol", "")).strip() == cleaned_symbol for item in constituents)

    return {
        "top_concentration": top_concentration,
        "coverage_weight": top_concentration,
        "coverage_ratio": 1.0,
        "coverage_count": len(constituents),
        "constituents": constituents,
        "matched_current_symbol": matched_current_symbol,
        "index_snapshot": dict(bundle.get("index_snapshot") or {}),
        "source": "index_topic_bundle",
        "fallback": str(bundle.get("fallback", "none") or "none"),
        "as_of": str(bundle.get("as_of", "")).strip(),
        "disclosure": "机构集中度代理优先复用已命中的指数成分权重主链，不重复走慢速指数代理聚合。",
    }


def _context_cn_index_concentration_proxy(
    keywords: Sequence[str],
    *,
    symbol: str = "",
    prefetched_bundle: Optional[Mapping[str, Any]] = None,
    context: Mapping[str, Any],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    cleaned_symbol = str(symbol).strip()
    cleaned_keywords = [str(item).strip() for item in keywords if str(item).strip()]
    bundle_proxy = _concentration_proxy_from_index_topic_bundle(prefetched_bundle or {}, symbol=cleaned_symbol)
    if bundle_proxy:
        return bundle_proxy
    primary_candidates = _context_cn_index_proxy_candidates(cleaned_keywords, context=context, config=config)
    for snapshot in primary_candidates:
        proxies = _context_cn_index_financial_proxies(
            str(snapshot.get("index_code", "")),
            top_n=5,
            context=context,
            config=config,
        )
        if not proxies:
            continue
        constituents = list(proxies.get("constituents") or [])
        if cleaned_symbol and any(str(item.get("symbol", "")).strip() == cleaned_symbol for item in constituents):
            merged = dict(proxies)
            merged["index_snapshot"] = dict(snapshot)
            merged["matched_current_symbol"] = True
            return merged
        if proxies.get("top_concentration") is not None:
            merged = dict(proxies)
            merged["index_snapshot"] = dict(snapshot)
            merged["matched_current_symbol"] = False
            return merged

    seen_codes = {str(item.get("index_code", "")).strip() for item in primary_candidates if str(item.get("index_code", "")).strip()}
    for keyword in cleaned_keywords:
        snapshot = _context_cn_index_snapshot([keyword], context, config)
        code = str(snapshot.get("index_code", "")).strip()
        if not snapshot or not code or code in seen_codes:
            continue
        seen_codes.add(code)
        proxies = _context_cn_index_financial_proxies(
            code,
            top_n=5,
            context=context,
            config=config,
        )
        if not proxies:
            continue
        constituents = list(proxies.get("constituents") or [])
        if cleaned_symbol and any(str(item.get("symbol", "")).strip() == cleaned_symbol for item in constituents):
            merged = dict(proxies)
            merged["index_snapshot"] = dict(snapshot)
            merged["matched_current_symbol"] = True
            return merged
        if proxies.get("top_concentration") is not None:
            merged = dict(proxies)
            merged["index_snapshot"] = dict(snapshot)
            merged["matched_current_symbol"] = False
            return merged
    return {}


def _valuation_keywords(
    metadata: Mapping[str, Any],
    asset_type: str = "",
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> List[str]:
    name = str(metadata.get("name", "")).strip()
    sector = str(metadata.get("sector", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    keywords: List[str] = []
    theme_keywords: List[str] = []
    if sector:
        theme_keywords.extend(THEME_INDEX_KEYWORD_MAP.get(sector, []))
    for item in chain_nodes:
        theme_keywords.append(item)
        theme_keywords.extend(THEME_INDEX_KEYWORD_MAP.get(item, []))

    if asset_type in {"cn_fund", "cn_etf"} and fund_profile:
        fund_keys = _fund_theme_keywords(metadata, fund_profile)
        benchmark_keys = _fund_benchmark_keywords(fund_profile)
        industry_keys = _fund_industry_keywords(fund_profile)
        holdings_text = " ".join(_fund_top_holding_names(fund_profile))
        semis_exposed = any(token in holdings_text for token in ("寒武纪", "中芯", "北方华创", "澜起", "长电", "韦尔", "兆易", "芯片", "半导体"))
        if semis_exposed:
            theme_keywords = [
                item
                for item in theme_keywords
                if str(item).strip() not in SEMICONDUCTOR_VALUATION_NOISY_KEYWORDS
            ]
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
        keywords.extend(theme_keywords)
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
    keywords.extend(theme_keywords)
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


def _infer_holdings_asset_type(holdings: Sequence[Mapping[str, Any]]) -> str:
    counts = {"cn_stock": 0, "hk": 0, "us": 0}
    for item in holdings:
        symbol = str(item.get("symbol", "") or item.get("股票代码", "")).strip()
        if not symbol:
            continue
        if re.fullmatch(r"\d{6}", symbol):
            counts["cn_stock"] += 1
        elif re.fullmatch(r"\d{1,5}", symbol):
            counts["hk"] += 1
        elif re.search(r"[A-Za-z]", symbol):
            counts["us"] += 1
    best = max(counts, key=counts.get)
    return best if counts[best] > 0 else ""


def _fund_holdings_valuation_proxy(
    collector: ValuationCollector,
    fund_profile: Optional[Mapping[str, Any]],
    *,
    top_n: int = 5,
) -> Dict[str, Any]:
    holdings = _fund_top_holdings(fund_profile, top_n=top_n)
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
    holdings_asset_type = _infer_holdings_asset_type(normalized_holdings)
    try:
        if holdings_asset_type == "cn_stock":
            return collector.get_weighted_stock_financial_proxies(normalized_holdings, top_n=top_n)
        if holdings_asset_type not in {"hk", "us"}:
            return {}
        return collector.get_weighted_market_financial_proxies(
            normalized_holdings,
            asset_type=holdings_asset_type,
            top_n=top_n,
        )
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
    industry_framework_label = str(metadata.get("industry_framework_label", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    text_blob = " ".join([name, sector, *chain_nodes])
    semiconductor_focused = _contains_any(text_blob, ["半导体", "芯片", "晶圆", "存储", "封装", "HBM", "Chiplet"])
    if sector == "金融":
        keywords = [
            industry_framework_label,
            *chain_nodes,
            sector,
            name,
        ]
    else:
        keywords = [
            *_valuation_keywords(metadata),
            *BOARD_MATCH_ALIASES.get(sector, []),
            sector,
            name,
            *chain_nodes,
        ]
    if semiconductor_focused:
        broad_sector_tokens = {"科技", "信息技术", "通信", "通信设备", "消费电子", "人工智能", "AI", "算力", "软件服务"}
        keywords = [
            keyword
            for keyword in keywords
            if str(keyword).strip() not in broad_sector_tokens or str(keyword).strip() == sector
        ]
        keywords = [
            "半导体",
            "芯片",
            "半导体设备",
            "芯片设备",
            "集成电路",
            "存储",
            "晶圆",
            "晶圆制造",
            "半导体材料",
            *keywords,
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


def _match_driver_row_with_score(
    frame: pd.DataFrame,
    metadata: Mapping[str, Any],
    name_candidates: Sequence[str],
) -> tuple[Optional[pd.Series], int]:
    if frame is None or frame.empty:
        return None, 0
    name_col = _first_column(frame, name_candidates)
    if not name_col:
        return None, 0
    keywords = [keyword.lower() for keyword in _board_keywords(metadata)]
    best_row: Optional[pd.Series] = None
    best_score = 0

    def _keyword_score(keyword: str, lowered_label: str) -> int:
        token = str(keyword or "").strip().lower()
        if not token:
            return 0
        if token == lowered_label:
            return 3
        # Two-character Chinese sector words like “消费 / 零售 / 旅游 / 科技”
        # are too broad for fuzzy contains matching; only longer tokens are allowed
        # to match by substring so “消费” no longer accidentally hits “消费电子”.
        if all("\u4e00" <= ch <= "\u9fff" for ch in token) and len(token) <= 2:
            return 0
        return 1 if token in lowered_label else 0

    for _, row in frame.iterrows():
        label = str(row.get(name_col, "")).strip()
        if not label:
            continue
        lowered = label.lower()
        score = sum(_keyword_score(keyword, lowered) for keyword in keywords)
        if score > best_score:
            best_row = row
            best_score = score
    return (best_row if best_score > 0 else None), best_score


def _match_driver_row(frame: pd.DataFrame, metadata: Mapping[str, Any], name_candidates: Sequence[str]) -> Optional[pd.Series]:
    matched, _score = _match_driver_row_with_score(frame, metadata, name_candidates)
    return matched


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


def _matched_sector_spot_row(
    metadata: Mapping[str, Any],
    drivers: Mapping[str, Any],
) -> tuple[Optional[pd.Series], pd.DataFrame, str]:
    matches: list[tuple[int, pd.Series, pd.DataFrame, str]] = []
    for level, frame in (
        ("industry", drivers.get("industry_spot", pd.DataFrame())),
        ("concept", drivers.get("concept_spot", pd.DataFrame())),
        ("dc_index", _driver_frame(drivers.get("dc_index"))),
    ):
        matched, score = _match_driver_row_with_score(frame, metadata, ("板块名称", "名称", "概念名称"))
        if matched is not None and score > 0:
            matches.append((score, matched, frame, level))
    if not matches:
        return None, pd.DataFrame(), ""
    _score, row, frame, level = sorted(matches, key=lambda item: item[0], reverse=True)[0]
    return row, frame, level


def _matched_sector_breadth_row(
    metadata: Mapping[str, Any],
    drivers: Mapping[str, Any],
) -> tuple[Optional[pd.Series], pd.DataFrame, str]:
    matches: list[tuple[int, int, pd.Series, pd.DataFrame, str]] = []
    for level, frame in (
        ("industry", drivers.get("industry_spot", pd.DataFrame())),
        ("concept", drivers.get("concept_spot", pd.DataFrame())),
        ("dc_index", _driver_frame(drivers.get("dc_index"))),
    ):
        matched, score = _match_driver_row_with_score(frame, metadata, ("板块名称", "名称", "概念名称"))
        if matched is None or score <= 0:
            continue
        advance_col = next((c for c in frame.columns if "上涨" in c and "家数" in c), None)
        decline_col = next((c for c in frame.columns if "下跌" in c and "家数" in c), None)
        has_counts = int(bool(advance_col and decline_col))
        matches.append((has_counts, score, matched, frame, level))
    if not matches:
        return None, pd.DataFrame(), ""
    _has_counts, _score, row, frame, level = sorted(matches, key=lambda item: (item[0], item[1]), reverse=True)[0]
    return row, frame, level


def _format_fundamental_floor_metric(label: str, value: Any, suffix: str = "%") -> str:
    try:
        return f"{label} {float(value):.1f}{suffix}"
    except (TypeError, ValueError):
        return label


def _fundamental_floor_snapshot(
    asset_type: str,
    metadata: Mapping[str, Any],
    fundamental_dimension: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> tuple[str, str, Optional[str], Optional[str]]:
    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        if _is_commodity_like_fund(asset_type, metadata, fund_profile):
            return (
                "ℹ️",
                "当前按商品/期货 ETF 的产品结构、跟踪标的和容量做基础质量判断，不使用股票财报底线。",
                None,
                None,
            )
        return ("ℹ️", "当前以 ETF / 行业代理为主，利润同比底线暂未接入原始财报数据", None, None)

    valuation_snapshot = dict(fundamental_dimension.get("valuation_snapshot") or {})
    financial_proxy = dict(fundamental_dimension.get("financial_proxy") or {})
    report_date = str(financial_proxy.get("report_date", "")).strip()
    growth_val = financial_proxy.get("profit_yoy")
    if growth_val is None:
        growth_val = financial_proxy.get("revenue_yoy")
    roe_val = financial_proxy.get("roe")
    margin_val = financial_proxy.get("gross_margin")
    cfps_val = financial_proxy.get("cfps")
    debt_val = financial_proxy.get("debt_to_assets")
    data_ready = any(value is not None for value in (growth_val, roe_val, margin_val, cfps_val, debt_val))
    if not data_ready:
        if valuation_snapshot:
            return ("ℹ️", "当前已接入真实估值，但增长/ROE/现金流/杠杆这些底线财务项覆盖还不完整。", None, None)
        return ("ℹ️", "当前财务快照覆盖不足，暂时无法把基本面底线做成硬排除。", None, None)

    severe_issues: List[str] = []
    soft_issues: List[str] = []
    preview_bits: List[str] = []
    growth_float = None
    cfps_float = None
    debt_float = None

    try:
        if growth_val is not None:
            growth_float = float(growth_val)
            preview_bits.append(_format_fundamental_floor_metric("增速", growth_float))
            if growth_float < 0:
                severe_issues.append(f"增长转负（{growth_float:.1f}%）")
            elif growth_float < 3:
                soft_issues.append(f"增长偏弱（{growth_float:.1f}%）")
    except (TypeError, ValueError):
        pass

    try:
        if roe_val is not None:
            roe_float = float(roe_val)
            preview_bits.append(_format_fundamental_floor_metric("ROE", roe_float))
            if roe_float < 0:
                severe_issues.append(f"ROE 为负（{roe_float:.1f}%）")
            elif roe_float < 5:
                severe_issues.append(f"ROE 过低（{roe_float:.1f}%）")
            elif roe_float < 8:
                soft_issues.append(f"ROE 偏低（{roe_float:.1f}%）")
    except (TypeError, ValueError):
        pass

    try:
        if margin_val is not None:
            margin_float = float(margin_val)
            preview_bits.append(_format_fundamental_floor_metric("毛利率", margin_float))
            if margin_float < 10:
                severe_issues.append(f"毛利率过低（{margin_float:.1f}%）")
            elif margin_float < 15:
                soft_issues.append(f"毛利率偏低（{margin_float:.1f}%）")
    except (TypeError, ValueError):
        pass

    try:
        if cfps_val is not None:
            cfps_float = float(cfps_val)
            preview_bits.append(_format_fundamental_floor_metric("每股经营现金流", cfps_float, ""))
            if cfps_float < -0.5:
                severe_issues.append(f"经营现金流明显为负（{cfps_float:.2f}）")
            elif cfps_float < 0:
                soft_issues.append(f"经营现金流小幅为负（{cfps_float:.2f}）")
    except (TypeError, ValueError):
        pass

    try:
        if debt_val is not None:
            debt_float = float(debt_val)
            preview_bits.append(_format_fundamental_floor_metric("资产负债率", debt_float))
            if debt_float >= 85:
                severe_issues.append(f"杠杆偏高（资产负债率 {debt_float:.1f}%）")
            elif debt_float >= 70:
                soft_issues.append(f"杠杆偏高（资产负债率 {debt_float:.1f}%）")
    except (TypeError, ValueError):
        pass

    prefix = f"报告期 {report_date}；" if report_date else ""
    preview = f"（{' / '.join(preview_bits[:3])}）" if preview_bits else ""
    critical_combo = (
        debt_float is not None
        and debt_float >= 90
        or (
            cfps_float is not None
            and cfps_float < -0.5
            and growth_float is not None
            and growth_float < 0
        )
    )
    if critical_combo or len(severe_issues) >= 2:
        issue_text = "；".join((severe_issues + soft_issues)[:3])
        return (
            "❌",
            f"{prefix}最新财务快照已触发明显底线压力：{issue_text}。",
            "基本面底线失守",
            "⚠️ 最新财务快照已经触发基本面底线，哪怕技术或催化不差，也更适合降级处理。",
        )
    if severe_issues or len(soft_issues) >= 2:
        issue_text = "；".join((severe_issues + soft_issues)[:3])
        return (
            "⚠️",
            f"{prefix}最新财务快照出现一定底线压力：{issue_text}。",
            None,
            "⚠️ 基本面底线已经出现压力，当前更适合把它当成需要额外验证而不是无条件放行的标的。",
        )
    return ("✅", f"{prefix}最新财务快照未见明显底线失守项{preview}。", None, None)


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


def _driver_frame(value: Any) -> pd.DataFrame:
    if isinstance(value, pd.DataFrame):
        return value
    if isinstance(value, Mapping):
        frame = value.get("frame")
        if isinstance(frame, pd.DataFrame):
            return frame
    return pd.DataFrame()


def _driver_row_or_single(frame: pd.DataFrame, metadata: Mapping[str, Any], name_candidates: Sequence[str]) -> Optional[pd.Series]:
    matched = _match_driver_row(frame, metadata, name_candidates)
    if matched is not None:
        return matched
    metadata_tokens = {
        str(metadata.get("name", "")).strip().lower(),
        str(metadata.get("symbol", "")).strip().lower(),
    }
    metadata_tokens.discard("")
    if metadata_tokens and not frame.empty:
        for column in name_candidates:
            if column not in frame.columns:
                continue
            for _, row in frame.iterrows():
                value = str(row.get(column, "")).strip().lower()
                if value and value in metadata_tokens:
                    return row
    return None


def _structure_auxiliary_rows(
    metadata: Mapping[str, Any],
    drivers: Mapping[str, Any],
    *,
    as_of: str,
) -> List[List[str]]:
    rows: List[List[str]] = []
    stock_name = str(metadata.get("name", "")).strip() or str(metadata.get("symbol", "")).strip()

    def _component_row(frame_keys: Sequence[str], name_candidates: Sequence[str]) -> Optional[pd.Series]:
        for key in frame_keys:
            frame = _driver_frame(drivers.get(key))
            if frame.empty:
                continue
            row = _driver_row_or_single(frame, metadata, name_candidates)
            if row is not None:
                return row
        return None

    def _framework_row(prefix: str, source_label: str, title_prefix: str) -> None:
        board_row = _component_row((f"{prefix}_board", f"{prefix}_industry", f"{prefix}_concept"), ("板块名称", "名称", "行业名称", "概念名称"))
        style_row = _component_row((f"{prefix}_style",), ("风格", "风格名称", "风格标签", "名称"))
        region_row = _component_row((f"{prefix}_region",), ("地区", "地域", "区域", "省份", "名称"))

        board_name = ""
        style_name = ""
        region_name = ""
        move_value: Optional[float] = None
        components: List[str] = []
        if board_row is not None:
            board_name = str(board_row.get(_first_column(pd.DataFrame([board_row]), ("板块名称", "名称", "行业名称", "概念名称")) or "", "")).strip()
            move_value = _row_number(board_row, ("涨跌幅", "今日涨跌幅", "涨跌幅(%)"))
            if board_name:
                components.append(board_name)
        if style_row is not None:
            style_name = str(style_row.get(_first_column(pd.DataFrame([style_row]), ("风格", "风格名称", "风格标签")) or "", "")).strip()
            if style_name:
                components.append(style_name)
        if region_row is not None:
            region_name = str(region_row.get(_first_column(pd.DataFrame([region_row]), ("地区", "地域", "区域", "省份")) or "", "")).strip()
            if region_name:
                components.append(region_name)
        if components:
            move_text = f"（{move_value:+.2f}%）" if move_value is not None else ""
            rows.append(
                [
                    as_of,
                    f"{title_prefix}：{stock_name} " + " / ".join(components) + move_text,
                    source_label,
                    "高" if move_value is not None and abs(move_value) >= 3 else "中",
                    board_name or style_name or region_name or stock_name,
                    "",
                    "标准结构归因",
                    (
                        f"偏利多，`{stock_name}` 的标准板块/风格/地区框架已可直接用来解释当前强弱。"
                        if move_value is None or move_value >= 0
                        else f"偏谨慎，`{stock_name}` 的标准板块/风格/地区框架当前显示承压。"
                    ),
                ]
            )

    _framework_row("tdx", "TDX结构专题", "TDX 结构框架")
    _framework_row("dc", "DC结构专题", "DC 结构框架")

    def _aux_row_from_frame(
        keys: Sequence[str],
        source_label: str,
        signal_type: str,
        title_prefix: str,
        *,
        name_candidates: Sequence[str],
        value_columns: Sequence[str],
        strength_columns: Sequence[str] = (),
        positive_detail: str,
        negative_detail: str,
    ) -> None:
        row: Optional[pd.Series] = None
        for key in keys:
            frame = _driver_frame(drivers.get(key))
            if frame.empty:
                continue
            row = _driver_row_or_single(frame, metadata, name_candidates)
            if row is not None:
                break
        if row is None:
            return
        matched_name = str(row.get(_first_column(pd.DataFrame([row]), name_candidates) or "", "")).strip() or stock_name
        value = ""
        for column in value_columns:
            text = str(row.get(column, "")).strip()
            if text and text != "—":
                value = text
                break
        if not value:
            for column in value_columns:
                numeric = _row_number(row, (column,))
                if numeric is not None:
                    if abs(numeric) >= 1e8:
                        value = f"{numeric / 1e8:.2f}亿"
                    elif abs(numeric) >= 1e4:
                        value = f"{numeric / 1e4:.2f}万"
                    else:
                        value = f"{numeric:.2f}"
                    break
        strength = "中"
        for column in strength_columns:
            numeric = _row_number(row, (column,))
            if numeric is not None:
                strength = "高" if abs(numeric) >= 5 else "中"
                break
        title = f"{title_prefix}：{matched_name}"
        if value:
            title += f" {value}"
        rows.append(
            [
                as_of,
                title,
                source_label,
                strength,
                matched_name,
                "",
                signal_type,
                positive_detail if strength != "低" else negative_detail,
            ]
        )

    _aux_row_from_frame(
        ("ggt_top10",),
        "港股/短线辅助",
        "港股通/CCASS辅助",
        "港股辅助层",
        name_candidates=("名称", "股票名称", "证券名称", "股份简称", "简称", "name"),
        value_columns=("持股数量", "持股市值", "持股比例", "占比", "排名", "net_amount", "amount", "rank"),
        strength_columns=("持股比例", "占比", "排名", "net_amount"),
        positive_detail="偏利多，港股通 / CCASS 命中后，先把它当作港股与短线辅助层，不把它写成正式主线。",
        negative_detail="偏中性，港股通 / CCASS 侧信号偏弱，当前仍以观察为主。",
    )
    _aux_row_from_frame(
        ("ccass_hold", "ccass"),
        "港股/短线辅助",
        "CCASS持股统计",
        "港股辅助层",
        name_candidates=("名称", "股票名称", "证券名称", "股份简称", "简称", "name", "col_participant_name"),
        value_columns=("持股数量", "持股比例", "持股市值", "占比", "shareholding", "share_ratio", "col_shareholding", "col_shareholding_percent"),
        strength_columns=("持股比例", "占比", "share_ratio", "col_shareholding_percent"),
        positive_detail="偏利多，CCASS 命中后只作为港股/短线辅助层，不把它写成确定性主线。",
        negative_detail="偏中性，CCASS 侧持仓证据不足，先按辅助信息理解。",
    )
    _aux_row_from_frame(
        ("hm_detail",),
        "港股/短线辅助",
        "港股持股明细",
        "港股辅助层",
        name_candidates=("名称", "股票名称", "证券名称", "股份简称", "简称", "ts_name", "hm_name"),
        value_columns=("持股数量", "持股比例", "持股市值", "占比", "net_amount", "buy_amount", "sell_amount"),
        strength_columns=("持股比例", "占比", "net_amount"),
        positive_detail="偏利多，持股明细已命中；对港股和短线只当辅助层参考。",
        negative_detail="偏中性，持股明细只够做辅助披露，不构成明确主线。",
    )
    _aux_row_from_frame(
        ("cb_issue",),
        "转债辅助层",
        "可转债辅助",
        "可转债辅助层",
        name_candidates=("名称", "股票名称", "证券简称", "发行人", "正股名称", "stk_short_name", "bond_short_name"),
        value_columns=("可转债简称", "转债简称", "债券简称", "转债代码", "债券代码", "余额", "剩余规模", "bond_short_name", "remain_size", "convert_ratio"),
        strength_columns=("余额", "剩余规模", "转股溢价率", "转股溢价", "remain_size", "convert_ratio"),
        positive_detail="偏利多，发行/申购/存量信息已命中；这里只把它当作可转债辅助层，不把缺口误写成主线确认。",
        negative_detail="偏中性，发行/申购/存量信息不足，仍按辅助层理解。",
    )
    _aux_row_from_frame(
        ("cb_share",),
        "转债辅助层",
        "可转债存量",
        "可转债辅助层",
        name_candidates=("名称", "股票名称", "证券简称", "发行人", "正股名称", "stk_short_name", "bond_short_name"),
        value_columns=("可转债简称", "转债简称", "债券简称", "转债代码", "债券代码", "余额", "剩余规模", "bond_short_name", "remain_size", "convert_ratio"),
        strength_columns=("余额", "剩余规模", "转股溢价率", "转股溢价", "remain_size", "convert_ratio"),
        positive_detail="偏利多，存量/余额信息已命中；转债层只作为辅助证据，不替代股票或 ETF 主链。",
        negative_detail="偏中性，存量/余额信息不足，先按辅助层理解。",
    )

    report_rc_frame = _driver_frame(drivers.get("report_rc"))
    if not report_rc_frame.empty:
        row = _driver_row_or_single(report_rc_frame, metadata, ("名称", "股票名称", "证券简称", "证券名称", "简称"))
        if row is not None:
            matched_name = str(row.get(_first_column(pd.DataFrame([row]), ("名称", "股票名称", "证券简称", "证券名称", "简称")) or "", "")).strip() or stock_name
            rating = ""
            for column in ("最新评级", "评级", "投资评级", "评级变动"):
                text = str(row.get(column, "")).strip()
                if text and text != "—":
                    rating = text
                    break
            title = f"研报辅助：{matched_name}"
            if rating:
                title += f" {rating}"
            report_signal = "研报评级/研究报告"
            strength = "中"
            lowered = rating.lower()
            if any(token in lowered for token in ("买入", "增持", "推荐", "强烈推荐")):
                strength = "高"
            elif any(token in lowered for token in ("卖出", "减持", "中性")):
                strength = "低"
            rows.append(
                [
                    as_of,
                    title,
                    "研报辅助层",
                    strength,
                    matched_name,
                    "",
                    report_signal,
                    (
                        "偏利多，研报评级/一致性已命中，只作为辅助证据，不替代正式公告或主线确认。"
                        if strength != "低"
                        else "偏中性，研报评级偏谨慎，先按辅助信息理解。"
                    ),
                ]
            )

    return rows


def _market_event_rows_from_context(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]] = None,
) -> List[List[str]]:
    config = dict(context.get("config") or {})
    drivers = _context_drivers(context, config)
    rows: List[List[str]] = []
    as_of = str(context.get("as_of", "")).strip()
    board_metadata = dict(metadata or {})
    asset_type = str(board_metadata.get("asset_type", "")).strip()
    if asset_type in {"cn_etf", "cn_fund"} and fund_profile:
        board_metadata["chain_nodes"] = _unique_strings(
            [
                *[str(item).strip() for item in board_metadata.get("chain_nodes", []) if str(item).strip()],
                *_fund_theme_keywords(board_metadata, fund_profile)[:4],
                *_fund_benchmark_keywords(fund_profile)[:3],
                *_fund_industry_keywords(fund_profile)[:3],
            ]
        )

    industry_index_snapshot = dict(board_metadata.get("industry_index_snapshot") or {})
    if not industry_index_snapshot:
        industry_index_snapshot = _context_industry_index_snapshot(board_metadata, context, fund_profile=fund_profile)
    if industry_index_snapshot:
        board_metadata = _enrich_metadata_with_industry_index_snapshot(board_metadata, industry_index_snapshot)
        rows.extend(_industry_index_rows_from_snapshot(board_metadata, industry_index_snapshot, as_of=as_of))
    if _asset_uses_index_topic_bundle(board_metadata, fund_profile=fund_profile, asset_type=asset_type):
        index_topic_bundle = dict(board_metadata.get("index_topic_bundle") or {})
        if not index_topic_bundle:
            index_topic_bundle = _context_index_topic_bundle(board_metadata, context, fund_profile=fund_profile)
        if index_topic_bundle:
            board_metadata = _enrich_metadata_with_index_topic_bundle(board_metadata, index_topic_bundle)
            rows.extend(_index_topic_rows_from_bundle(board_metadata, index_topic_bundle, as_of=as_of))
    etf_proxy_rows: List[List[str]] = []
    if str(board_metadata.get("asset_type", "")).strip() in {"cn_etf", "cn_fund"} and fund_profile:
        etf_proxy_rows = _etf_profile_proxy_rows(
            board_metadata,
            fund_profile,
            as_of=as_of,
            has_index_weight_row=any(str(row[2] if len(row) > 2 else "").strip() == "指数成分/权重" for row in rows),
                has_standard_industry_row=any(
                    str(row[2] if len(row) > 2 else "").strip() in {"申万行业框架", "中信行业框架"}
                    for row in rows
                ),
            )
    rows.extend(_structure_auxiliary_rows(board_metadata, drivers, as_of=as_of))
    standard_impacts = {
        str(item.get("index_name", "")).strip()
        for item in list(industry_index_snapshot.get("items") or [])
        if str(item.get("index_name", "")).strip()
    }
    deferred_rows: List[List[str]] = []

    board_row, frame, level = _matched_sector_spot_row(board_metadata, drivers)
    if board_row is not None and frame is not None and not frame.empty:
        name_col = _first_column(frame, ("板块名称", "名称", "概念名称"))
        move_value = _row_number(board_row, ("涨跌幅", "今日涨跌幅"))
        if name_col and move_value is not None:
            board_name = str(board_row.get(name_col, "")).strip()
            skip_generic_industry_row = (
                level == "industry"
                and board_name
                and any(
                    board_name == impact or board_name in impact or impact in board_name
                    for impact in standard_impacts
                )
            )
            leader_col = _first_column(frame, ("领涨股票", "领涨股", "领涨证券"))
            leader_name = str(board_row.get(leader_col, "")).strip() if leader_col else ""
            if not skip_generic_industry_row:
                title_prefix = (
                    "A股概念领涨"
                    if move_value >= 0 and level == "concept"
                    else "A股概念承压"
                    if level == "concept"
                    else "A股行业走强"
                    if move_value >= 0
                    else "A股行业承压"
                )
                source_label = "A股概念/盘面" if level == "concept" else "A股行业/盘面"
                title = f"{title_prefix}：{board_name}（{move_value:+.2f}%）"
                if leader_name:
                    title += f"；领涨 {leader_name}"
                abs_move = abs(move_value)
                if abs_move >= 3:
                    strength = "高"
                elif abs_move >= 1:
                    strength = "中"
                else:
                    strength = "低"
                signal_type = "主线增强" if move_value >= 0 else "主线承压"
                conclusion = (
                    f"偏利多，先看 `{board_name}` 能否继续扩散。"
                    if move_value >= 0
                    else f"偏谨慎，先看 `{board_name}` 是否继续走弱。"
                )
                deferred_rows.append([as_of, title, source_label, strength, board_name, "", signal_type, conclusion])

    hot_rank = _hot_rank_snapshot(board_metadata, drivers)
    hot_name = str(hot_rank.get("name", "")).strip()
    hot_rank_value = hot_rank.get("rank")
    if hot_name and hot_rank_value is not None:
        deferred_rows.append(
            [
                as_of,
                f"A股热股前排：{hot_name}（热度排名 {int(hot_rank_value)}）",
                "A股热股/盘面",
                "中",
                hot_name,
                "",
                "热度抬升",
                f"偏利多，但先看 `{hot_name}` 能否把热度转成价格和成交确认。",
            ]
        )

    if str(board_metadata.get("asset_type", "")).strip() == "cn_stock":
        theme_membership = _cn_stock_theme_membership_snapshot(board_metadata, context)
        for item in list(theme_membership.get("items") or [])[:2]:
            board_name = str(item.get("board_name", "")).strip()
            board_type_label = str(item.get("board_type_label", "题材")).strip() or "题材"
            stock_name = str(board_metadata.get("name", "")).strip() or str(board_metadata.get("symbol", "")).strip()
            if not board_name or not stock_name:
                continue
            pct_change = item.get("pct_change")
            move_text = f"（{float(pct_change):+.2f}%）" if pct_change is not None else ""
            signal_type = "主线归因" if pct_change is None or float(pct_change) >= 0 else "主题回落"
            conclusion = (
                f"偏利多，`{stock_name}` 属于 `{board_name}` 链路，当前可直接按主题成员去解释它的强弱。"
                if signal_type == "主线归因"
                else f"偏谨慎，`{stock_name}` 虽属 `{board_name}` 链路，但当前主题本身在回落。"
            )
            rows.append(
                [
                    as_of,
                    f"A股{board_type_label}成员：{stock_name} 属于 {board_name}{move_text}",
                    "同花顺主题成分",
                    str(item.get("signal_strength", "中")).strip() or "中",
                    board_name,
                    "",
                    signal_type,
                    conclusion,
                ]
            )

        regulatory_snapshot = _cn_stock_regulatory_risk_snapshot(board_metadata, context)
        components = dict(regulatory_snapshot.get("components") or {})
        stock_name = str(board_metadata.get("name", "")).strip() or str(board_metadata.get("symbol", "")).strip()
        if bool(regulatory_snapshot.get("active_st")):
            detail = str(dict(components.get("stock_st") or {}).get("detail", "")).strip() or str(regulatory_snapshot.get("detail", "")).strip()
            rows.append(
                [
                    as_of,
                    f"ST 风险提示：{stock_name} 当前仍在风险警示板名单",
                    "交易所风险专题",
                    "高",
                    stock_name,
                    "",
                    "风险提示",
                    detail or "偏谨慎，当前直接按 ST / *ST 高风险样本处理，不把热度写成可执行催化。",
                ]
            )
        elif int(regulatory_snapshot.get("active_alert_count") or 0) > 0:
            detail = str(dict(components.get("stk_alert") or {}).get("detail", "")).strip() or str(regulatory_snapshot.get("detail", "")).strip()
            rows.append(
                [
                    as_of,
                    f"交易所重点提示：{stock_name} 当前仍在重点提示证券名单",
                    "交易所风险专题",
                    "中",
                    stock_name,
                    "",
                    "风险提示",
                    detail or "偏谨慎，先按高波动/高关注样本管理仓位和节奏。",
                ]
            )
        elif int(regulatory_snapshot.get("high_shock_count") or 0) > 0:
            detail = str(dict(components.get("stk_high_shock") or {}).get("detail", "")).strip() or str(regulatory_snapshot.get("detail", "")).strip()
            rows.append(
                [
                    as_of,
                    f"异常波动提示：{stock_name} 近窗口命中过严重异常波动",
                    "交易所风险专题",
                    "中",
                    stock_name,
                    "",
                    "风险提示",
                    detail or "偏谨慎，先把它当成高波动样本，而不是普通趋势延续。",
                ]
            )

        chip_snapshot = _cn_stock_chip_snapshot(board_metadata, context)
        chip_trade_gap_days = pd.to_numeric(pd.Series([chip_snapshot.get("trade_gap_days")]), errors="coerce").dropna()
        chip_is_t1_direct = (
            str(chip_snapshot.get("status", "")).strip() == "matched"
            and not bool(chip_snapshot.get("is_fresh"))
            and not chip_trade_gap_days.empty
            and int(chip_trade_gap_days.iloc[0]) <= 1
        )
        if str(chip_snapshot.get("status", "")).strip() == "matched" and (bool(chip_snapshot.get("is_fresh")) or chip_is_t1_direct):
            winner_rate = pd.to_numeric(pd.Series([chip_snapshot.get("winner_rate_pct")]), errors="coerce").dropna()
            price_vs_avg = pd.to_numeric(pd.Series([chip_snapshot.get("price_vs_weight_avg_pct")]), errors="coerce").dropna()
            above_price = pd.to_numeric(pd.Series([chip_snapshot.get("above_price_pct")]), errors="coerce").dropna()
            if not winner_rate.empty and not price_vs_avg.empty and float(winner_rate.iloc[0]) >= 65 and float(price_vs_avg.iloc[0]) >= 0:
                prefix = "筹码确认" if bool(chip_snapshot.get("is_fresh")) else "上一交易日筹码确认"
                suffix = "" if bool(chip_snapshot.get("is_fresh")) else "（T+1 直连）"
                rows.append(
                    [
                        as_of,
                        f"{prefix}：{stock_name} 胜率约 {float(winner_rate.iloc[0]):.1f}%，现价已回到平均成本上方{suffix}",
                        "筹码分布专题",
                        "中",
                        stock_name,
                        "",
                        "筹码确认",
                        str(chip_snapshot.get("detail", "")).strip() or "偏利多，真实筹码分布开始配合价格修复。",
                    ]
                )
            elif not above_price.empty and float(above_price.iloc[0]) >= 60:
                prefix = "筹码压力提示" if bool(chip_snapshot.get("is_fresh")) else "上一交易日筹码压力提示"
                suffix = "" if bool(chip_snapshot.get("is_fresh")) else "（T+1 直连）"
                rows.append(
                    [
                        as_of,
                        f"{prefix}：{stock_name} 上方套牢盘约 {float(above_price.iloc[0]):.1f}%{suffix}",
                        "筹码分布专题",
                        "中",
                        stock_name,
                        "",
                        "筹码承压",
                        str(chip_snapshot.get("detail", "")).strip() or "偏谨慎，真实筹码分布仍提示上方抛压偏重。",
                    ]
                )

        capital_flow_snapshot = _cn_stock_capital_flow_snapshot(board_metadata, context)
        flow_status = str(capital_flow_snapshot.get("status", "")).strip()
        flow_is_fresh = bool(capital_flow_snapshot.get("is_fresh"))
        direct_main_flow = pd.to_numeric(pd.Series([capital_flow_snapshot.get("direct_main_flow")]), errors="coerce").dropna()
        direct_trade_gap_days = pd.to_numeric(pd.Series([capital_flow_snapshot.get("direct_trade_gap_days")]), errors="coerce").dropna()
        board_main_flow = pd.to_numeric(pd.Series([capital_flow_snapshot.get("board_main_flow")]), errors="coerce").dropna()
        direct_t1_ready = not direct_main_flow.empty and not direct_trade_gap_days.empty and int(direct_trade_gap_days.iloc[0]) <= 1
        if (flow_is_fresh and flow_status in {"matched", "proxy"}) or direct_t1_ready:
            if not direct_main_flow.empty and abs(float(direct_main_flow.iloc[0])) >= 50_000_000:
                prefix = "个股资金流确认" if flow_is_fresh and flow_status in {"matched", "proxy"} else "上一交易日个股资金流确认"
                suffix = "" if flow_is_fresh and flow_status in {"matched", "proxy"} else "（T+1 直连）"
                flow_window_label = "当日" if flow_is_fresh and flow_status in {"matched", "proxy"} else ""
                rows.append(
                    [
                        as_of,
                        f"{prefix}：{stock_name} {flow_window_label}主力净{'流入' if float(direct_main_flow.iloc[0]) >= 0 else '流出'} {_fmt_yi_number(float(direct_main_flow.iloc[0]))}{suffix}",
                        "个股资金流向专题",
                        "中" if abs(float(direct_main_flow.iloc[0])) < 200_000_000 else "高",
                        stock_name,
                        "",
                        "资金承接" if float(direct_main_flow.iloc[0]) >= 0 else "资金承压",
                        str(capital_flow_snapshot.get("detail", "")).strip()
                        or ("偏利多，个股主力资金开始给出直接承接。" if float(direct_main_flow.iloc[0]) >= 0 else "偏谨慎，个股主力资金仍在净流出。"),
                    ]
                )
            elif not board_main_flow.empty and abs(float(board_main_flow.iloc[0])) >= 100_000_000:
                board_name = str(capital_flow_snapshot.get("board_name", "")).strip() or "相关主题"
                rows.append(
                    [
                        as_of,
                        f"主题资金共振：{board_name} 主力净{'流入' if float(board_main_flow.iloc[0]) >= 0 else '流出'} {_fmt_yi_number(float(board_main_flow.iloc[0]))}",
                        "个股资金流向专题",
                        "中",
                        board_name,
                        "",
                        "主题资金共振" if float(board_main_flow.iloc[0]) >= 0 else "主题资金回落",
                        str(capital_flow_snapshot.get("detail", "")).strip()
                        or ("偏利多，当前先由行业/概念资金流代理支撑。" if float(board_main_flow.iloc[0]) >= 0 else "偏谨慎，相关主题资金流仍在回落。"),
                    ]
                )

        broker_snapshot = _cn_stock_broker_recommend_snapshot(board_metadata, context)
        broker_status = str(broker_snapshot.get("status", "")).strip()
        broker_count = int(broker_snapshot.get("latest_broker_count") or 0)
        broker_delta = broker_snapshot.get("broker_delta")
        broker_crowding = str(broker_snapshot.get("crowding_level", "")).strip()
        broker_month = str(broker_snapshot.get("latest_date", "")).strip()
        if bool(broker_snapshot.get("is_fresh")) and broker_status == "matched" and broker_count >= 2:
            if broker_crowding == "high":
                rows.append(
                    [
                        as_of,
                        f"卖方预期过热：{stock_name} 本月获 {broker_count} 家券商金股推荐",
                        "卖方共识专题",
                        "中",
                        stock_name,
                        "",
                        "卖方预期过热",
                        str(broker_snapshot.get("detail", "")).strip()
                        or "偏谨慎，卖方月度金股覆盖已经偏密，后续更需要业绩或订单验证，而不是只靠一致预期续推。",
                    ]
                )
            else:
                signal_label = "卖方共识升温" if broker_delta is not None and float(broker_delta) > 0 else "卖方覆盖提升"
                signal_title = (
                    f"卖方共识升温：{stock_name} 本月获 {broker_count} 家券商金股推荐"
                    if signal_label == "卖方共识升温"
                    else f"卖方覆盖提升：{stock_name} 本月获 {broker_count} 家券商金股推荐"
                )
                rows.append(
                    [
                        as_of,
                        signal_title,
                        "卖方共识专题",
                        "高" if broker_count >= 5 else "中",
                        stock_name,
                        "",
                        signal_label,
                        str(broker_snapshot.get("detail", "")).strip()
                        or "偏利多，卖方月度金股覆盖开始抬升，但这里只当共识热度参考，不替代公司级强催化。",
                    ]
                )
        elif broker_status == "stale" and broker_month:
            rows.append(
                [
                    as_of,
                    f"卖方共识非当期：{stock_name} 最新券商金股仍停在 {broker_month}",
                    "卖方共识专题",
                    "低",
                    stock_name,
                    "",
                    "卖方共识观察",
                    str(broker_snapshot.get("detail", "")).strip()
                    or "偏中性，卖方月度金股只命中历史月份，本轮不把旧共识写成本月 fresh 升温。",
                ]
            )

        margin_snapshot = _cn_stock_margin_snapshot(board_metadata, context)
        margin_level = str(margin_snapshot.get("crowding_level", "")).strip()
        if bool(margin_snapshot.get("is_fresh")) and margin_level in {"high", "medium"}:
            rows.append(
                [
                    as_of,
                    f"两融拥挤提示：{stock_name} 当前融资盘{'升温明显' if margin_level == 'high' else '仍在升温'}",
                    "两融专题",
                    "高" if margin_level == "high" else "中",
                    stock_name,
                    "",
                    "两融拥挤",
                    str(margin_snapshot.get("detail", "")).strip()
                    or "偏谨慎，融资盘一致性交易会放大短线波动。",
                ]
            )

        board_action_snapshot = _cn_stock_board_action_snapshot(board_metadata, context)
        if bool(board_action_snapshot.get("is_fresh")):
            if bool(board_action_snapshot.get("has_positive_signal")):
                rows.append(
                    [
                        as_of,
                        f"打板信号确认：{stock_name} {'/'.join(list(board_action_snapshot.get('positive_bits') or [])[:2])}",
                        "龙虎榜/打板专题",
                        "中",
                        stock_name,
                        "",
                        "龙虎榜确认",
                        str(board_action_snapshot.get("detail", "")).strip() or "偏利多，微观交易结构开始配合。",
                    ]
                )
            elif bool(board_action_snapshot.get("has_negative_signal")):
                rows.append(
                    [
                        as_of,
                        f"打板风险提示：{stock_name} {'/'.join(list(board_action_snapshot.get('negative_bits') or [])[:2])}",
                        "龙虎榜/打板专题",
                        "中",
                        stock_name,
                        "",
                        "打板过热",
                        str(board_action_snapshot.get("detail", "")).strip() or "偏谨慎，打板/情绪交易风险偏高。",
                    ]
                )

    rows.extend(deferred_rows)
    rows.extend(etf_proxy_rows)
    return _trim_market_event_rows(rows, limit=5)


def _trim_market_event_rows(rows: Sequence[Sequence[Any]], *, limit: int) -> List[List[Any]]:
    normalized = [list(row) for row in rows if list(row)]
    if len(normalized) <= limit:
        return normalized

    def _priority(row: Sequence[Any], index: int) -> tuple[int, int, int, int]:
        source = str(row[2] if len(row) > 2 else "").strip()
        signal = str(row[6] if len(row) > 6 else "").strip()
        strength = str(row[3] if len(row) > 3 else "").strip()
        source_rank = 5
        if source in {"公司公告/结构化", "互动易/投资者关系"}:
            source_rank = 0
        elif source == "卖方共识专题":
            source_rank = 1
        elif source in {"交易所风险专题", "个股资金流向专题", "两融专题", "龙虎榜/打板专题", "筹码分布专题"}:
            source_rank = 2
        elif source in {"TDX结构专题", "DC结构专题", "港股/短线辅助", "转债辅助层", "研报辅助层"}:
            source_rank = 3
        elif source in {"申万行业框架", "中信行业框架", "同花顺主题成分"}:
            source_rank = 4
        elif source in {"相关指数/框架", "指数技术面", "指数成分/权重"}:
            source_rank = 5
        signal_rank = 1
        if signal in {"管理层口径确认", "公司级直接情报", "卖方共识升温", "卖方覆盖提升", "卖方共识观察"}:
            signal_rank = 0
        strength_rank = {"高": 0, "中": 1, "低": 2}.get(strength, 1)
        return (source_rank, signal_rank, strength_rank, index)

    ranked = sorted(enumerate(normalized), key=lambda item: _priority(item[1], item[0]))
    selected = [row for _, row in ranked[:limit]]
    overflow = [row for _, row in ranked[limit:]]
    has_risk = any(str(row[2] if len(row) > 2 else "").strip() == "交易所风险专题" for row in selected)
    if has_risk:
        return selected

    risk_row = next(
        (row for row in overflow if str(row[2] if len(row) > 2 else "").strip() == "交易所风险专题"),
        None,
    )
    if not risk_row:
        return selected

    def _replace_first(predicate) -> bool:
        for idx in range(len(selected) - 1, -1, -1):
            if predicate(selected[idx]):
                selected[idx] = risk_row
                return True
        return False

    replaced = (
        _replace_first(lambda row: str(row[2] if len(row) > 2 else "").strip() == "相关指数/框架")
        or _replace_first(lambda row: str(row[2] if len(row) > 2 else "").strip() == "指数成分/权重")
        or _replace_first(lambda row: str(row[2] if len(row) > 2 else "").strip() == "指数技术面")
    )
    if not replaced:
        selected[-1] = risk_row
    return selected


@lru_cache(maxsize=8)
def _load_catalyst_profiles(path_value: str) -> Dict[str, Any]:
    path = resolve_project_path(path_value)
    payload = load_yaml(path, default={"profiles": {}}) or {"profiles": {}}
    return dict(payload.get("profiles", {}))


@lru_cache(maxsize=1)
def _known_sector_buckets() -> set[str]:
    profiles = _load_catalyst_profiles("config/catalyst_profiles.yaml")
    return {
        *[str(item).strip() for item in GENERIC_CATALYST_PROFILES if str(item).strip()],
        *[str(item).strip() for item in BOARD_MATCH_ALIASES if str(item).strip()],
        *[str(item).strip() for item in profiles if str(item).strip()],
    }


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


def _catalyst_factor_maxima(
    profile: Mapping[str, Any],
    *,
    asset_type: str,
    is_individual_stock: bool,
) -> Dict[str, int]:
    maxima = {
        "policy": 25 if (asset_type == "cn_stock" and is_individual_stock) else 30,
        "leader": 15 if (asset_type == "cn_stock" and is_individual_stock) else 25,
        "structured": 15,
        "overseas": 20,
        "news_density": 10,
        "news_heat": 10,
        "forward_event": 5,
        "directional": 12,
    }
    if not (asset_type == "cn_stock" and is_individual_stock):
        return maxima
    profile_keys = {
        str(profile.get("profile_name", "")).strip(),
        str(profile.get("sector_hint", "")).strip(),
    }
    if not any(key in CN_STOCK_CATALYST_OVERRIDE_PROFILES for key in profile_keys if key):
        return maxima
    overrides = dict(profile.get("factor_max_overrides") or {})
    for key, value in overrides.items():
        if key not in maxima:
            continue
        try:
            maxima[key] = max(int(value), 0)
        except (TypeError, ValueError):
            continue
    return maxima


def _rescale_catalyst_award(awarded: int, current_max: int, target_max: int) -> int:
    if awarded == 0 or current_max <= 0 or target_max == current_max:
        return int(awarded)
    if target_max <= 0:
        return 0
    scaled = int(round(abs(float(awarded)) / float(current_max) * float(target_max)))
    if abs(awarded) > 0 and scaled == 0:
        scaled = 1
    scaled = min(int(target_max), scaled)
    return scaled if awarded > 0 else -scaled


_FUND_DIRECTIONAL_GENERIC_TERMS = {
    "etf",
    "基金",
    "联接",
    "指数",
    "行业",
    "主题",
    "综合",
    "宽基",
}
_FUND_DIRECTIONAL_CUE_TOKENS = (
    "大涨",
    "走强",
    "活跃",
    "催化",
    "景气",
    "上修",
    "订单",
    "中标",
    "回暖",
    "提价",
    "放量",
    "获批",
    "临床",
    "授权",
    "license-out",
    "bd",
    "首付款",
    "里程碑",
    "扩产",
    "开支",
    "capex",
    "建设",
    "推进",
)


def _is_specific_fund_directional_term(term: str) -> bool:
    cleaned = str(term or "").strip().lower()
    if len(cleaned) < 2:
        return False
    return cleaned not in _FUND_DIRECTIONAL_GENERIC_TERMS


def _fund_directional_catalyst_signal(
    news_pool: Sequence[Mapping[str, Any]],
    fund_profile: Optional[Mapping[str, Any]],
    *,
    metadata: Optional[Mapping[str, Any]] = None,
    profile: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    benchmark_keywords = _fund_benchmark_keywords(fund_profile)[:4]
    industry_keywords = _fund_industry_keywords(fund_profile)[:5]
    holding_names = _fund_top_holding_names(fund_profile, top_n=5)
    metadata_payload = dict(metadata or {})
    profile_payload = dict(profile or {})
    instrument_tokens = _instrument_identity_tokens(metadata_payload)
    current_symbol = "".join(ch for ch in str(metadata_payload.get("symbol", "")).strip() if ch.isdigit())
    theme_keywords = _unique_strings(
        [
            str(metadata_payload.get("sector", "")).strip(),
            *[str(item).strip() for item in metadata_payload.get("chain_nodes", []) if str(item).strip()],
            str(profile_payload.get("profile_name", "")).strip(),
            *[str(item).strip() for item in profile_payload.get("themes", []) if str(item).strip()],
            *[str(item).strip() for item in _theme_news_expansion_terms(metadata_payload, profile_payload) if str(item).strip()],
        ]
    )
    theme_keywords = [
        item
        for item in theme_keywords
        if item and item not in {"ETF", "基金", "指数", "主题", "行业", "综合", "老龄化", "医药"}
    ][:6]
    if not (benchmark_keywords or industry_keywords or holding_names or theme_keywords):
        return {}

    candidates: List[tuple[int, int, int, int, Mapping[str, Any], List[str], List[str]]] = []
    for item in news_pool:
        if _is_non_positive_company_statement(item):
            continue
        text = _headline_text(item)
        matched_groups: List[str] = []
        matched_terms: List[str] = []
        for label, tokens in (
            ("跟踪基准", benchmark_keywords),
            ("行业暴露", industry_keywords),
            ("核心成分", holding_names),
            ("主题线索", theme_keywords),
        ):
            hits = [token for token in tokens if _contains_any(text, [token])]
            if hits:
                matched_groups.append(label)
                matched_terms.append(hits[0])
        if not matched_groups:
            continue
        direct_groups = [label for label in matched_groups if label in {"跟踪基准", "行业暴露", "核心成分"}]
        title_text = _title_source_text(item)
        has_identity_hit = bool(instrument_tokens and _contains_any(title_text, instrument_tokens))
        has_index_marker = any(token in title_text for token in ("标的指数", "跟踪指数"))
        has_fund_marker = any(token in title_text for token in ("ETF", "基金", "联接"))
        mentioned_codes = re.findall(r"(?<!\d)(\d{5,6})(?!\d)", title_text)
        mentions_other_fund_code = bool(mentioned_codes) and (
            not current_symbol or any(code != current_symbol for code in mentioned_codes)
        )
        if mentions_other_fund_code and not has_identity_hit:
            continue
        has_benchmark_or_industry_group = any(label in {"跟踪基准", "行业暴露"} for label in matched_groups)
        has_product_marker = has_index_marker or (
            has_fund_marker and (has_identity_hit or has_benchmark_or_industry_group)
        )
        specific_matched_terms = [term for term in matched_terms if _is_specific_fund_directional_term(term)]
        sector_directional_hit = (
            not has_identity_hit
            and not has_product_marker
            and not has_fund_marker
            and not mentions_other_fund_code
            and any(label in {"行业暴露", "主题线索"} for label in matched_groups)
            and bool(specific_matched_terms)
            and _contains_any(title_text, _FUND_DIRECTIONAL_CUE_TOKENS)
        )
        if not has_identity_hit and not has_product_marker and not sector_directional_hit:
            continue
        if not direct_groups and not (has_identity_hit or has_product_marker or sector_directional_hit):
            continue
        if direct_groups == ["核心成分"] and not has_identity_hit:
            continue
        if len(direct_groups) >= 2:
            award = 12 if has_identity_hit else 10
        elif direct_groups and (has_identity_hit or has_product_marker):
            award = 8
        elif sector_directional_hit and direct_groups:
            award = 6
        elif has_identity_hit and "主题线索" in matched_groups:
            award = 6
        elif sector_directional_hit and "主题线索" in matched_groups:
            award = 5
        elif has_product_marker and "主题线索" in matched_groups and not mentions_other_fund_code:
            award = 5
        else:
            continue
        candidates.append(
            (
                award,
                1 if has_identity_hit else 0,
                len(direct_groups),
                len(set(matched_terms)),
                item,
                matched_groups,
                matched_terms,
            )
        )

    if not candidates:
        return {}

    award, _identity_flag, direct_group_count, term_count, item, matched_groups, matched_terms = max(
        candidates,
        key=lambda row: (row[0], row[1], row[2], row[3]),
    )
    unique_terms = list(dict.fromkeys(matched_terms))
    return {
        "item": item,
        "award": award,
        "matched_groups": matched_groups,
        "matched_terms": unique_terms,
    }


def _select_fund_directional_news_pool(
    recent_theme_news_pool: Sequence[Mapping[str, Any]],
    dynamic_related_news: Sequence[Mapping[str, Any]],
    all_news_pool: Sequence[Mapping[str, Any]],
    fund_profile: Optional[Mapping[str, Any]],
    *,
    metadata: Optional[Mapping[str, Any]] = None,
    profile: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    recent_snapshot = _fund_directional_catalyst_signal(
        recent_theme_news_pool,
        fund_profile,
        metadata=metadata,
        profile=profile,
    )
    if recent_snapshot:
        return recent_snapshot
    if dynamic_related_news:
        dynamic_snapshot = _fund_directional_catalyst_signal(
            dynamic_related_news,
            fund_profile,
            metadata=metadata,
            profile=profile,
        )
        if dynamic_snapshot:
            dynamic_snapshot = dict(dynamic_snapshot)
            dynamic_snapshot["fallback_scope"] = "dynamic_related_news"
            return dynamic_snapshot
    if (recent_theme_news_pool or dynamic_related_news) and all_news_pool:
        all_snapshot = _fund_directional_catalyst_signal(
            all_news_pool,
            fund_profile,
            metadata=metadata,
            profile=profile,
        )
        if all_snapshot:
            all_snapshot = dict(all_snapshot)
            all_snapshot["fallback_scope"] = "all_news_pool"
            return all_snapshot
    return recent_snapshot


def _uses_domestic_sector_proxy(
    asset_type: str,
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
) -> bool:
    if asset_type not in {"cn_etf", "cn_fund"}:
        return True
    fund_profile = dict(context.get("fund_profile") or {})
    style = dict(fund_profile.get("style") or {})
    taxonomy = dict(style.get("taxonomy") or {})
    exposure_scope = str(taxonomy.get("exposure_scope", "")).strip()
    if exposure_scope == "跨境":
        return False
    benchmark_note = str(style.get("benchmark_note") or "").strip().lower()
    text = " ".join(
        [
            benchmark_note,
            str(metadata.get("name", "")).strip().lower(),
            str(metadata.get("sector", "")).strip().lower(),
            " ".join(str(item).strip().lower() for item in metadata.get("chain_nodes", []) if str(item).strip()),
        ]
    )
    if any(token in text for token in ("港股", "qdii", "美股", "海外", "纳斯达克", "恒生", "hong kong", "nasdaq")):
        return False
    return True


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

    exact_sector_profile = dict(_load_catalyst_profiles("config/catalyst_profiles.yaml").get(sector, {}))
    if exact_sector_profile:
        profile = _merge_catalyst_profiles(GENERIC_CATALYST_PROFILES.get(sector, {}), exact_sector_profile)
        profile["profile_name"] = sector
        profile.setdefault("sector_hint", sector)
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
    if any(node in chain_nodes for node in ("卫星通信", "卫星互联网", "商业航天")):
        return stock_base + ["卫星通信", "卫星互联网", "商业航天", "低轨卫星", "火箭发射", "组网", "频轨", "航天", "satellite", "launch"]
    if any(node in chain_nodes for node in ("数据中心", "AI算力")) and sector == "通信":
        return stock_base + ["数据中心", "idc", "通信设备", "交换机", "以太网", "ai服务器", "东数西算", "capex"]
    if any(node in chain_nodes for node in ("运营商", "通信服务", "5G/6G")):
        return stock_base + ["运营商", "电信", "5g", "6g", "万兆光网", "云网", "用户数", "ARPU", "资本开支"]
    if sector == "通信":
        return stock_base + ["通信", "光模块", "光通信", "cpo", "800g", "1.6t", "交换机", "以太网", "数据中心", "idc", "5g", "6g", "运营商", "电信", "capex"]
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
    if sector == "传媒":
        return stock_base + ["传媒", "游戏", "动漫", "影视", "aigc", "ai应用", "版号", "广告", "出海"]
    if sector == "科技":
        return stock_base + ["科技", "ai", "算力", "软件", "云计算", "互联网", "PCB", "机器人", "capex"]
    if "纳斯达克" in name or "纳指" in name:
        return stock_base + ["nasdaq", "纳斯达克", "纳指", "big tech", "earnings", "guidance", "ai"]
    result = _metadata_news_keys(metadata)
    return stock_base + [k for k in result if k not in stock_base]


def _theme_news_expansion_terms(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> List[str]:
    text = " ".join(
        [
            str(metadata.get("name", "")).strip(),
            str(metadata.get("sector", "")).strip(),
            str(metadata.get("industry", "")).strip(),
            " ".join(str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()),
            str(profile.get("profile_name", "")).strip(),
            " ".join(str(item).strip() for item in profile.get("themes", []) if str(item).strip()),
        ]
    )
    terms: List[str] = []
    if _contains_any(text, ["半导体", "芯片", "晶圆", "存储", "封装", "HBM", "Chiplet"]):
        terms.extend(["资本开支", "先进封装", "HBM", "Chiplet", "晶圆厂", "AI服务器", "先进制程", "封测", "设备链", "存储"])
    if _contains_any(text, ["卫星通信", "卫星互联网", "商业航天", "低轨卫星"]):
        terms.extend(["发射", "组网", "低轨卫星", "卫星互联网", "商业航天", "卫星终端"])
    if _contains_any(text, ["通信", "光模块", "光通信", "CPO", "数据中心", "IDC", "运营商", "5G", "6G"]):
        terms.extend(["800G", "1.6T", "以太网", "交换机", "万兆光网", "东数西算", "AI服务器", "资本开支", "订单", "扩产"])
    if _contains_any(text, ["黄金", "有色", "铜", "铝", "资源"]):
        terms.extend(["金价", "铜价", "铝价", "矿业资本开支", "供给扰动", "避险"])
    if _contains_any(text, ["原油", "煤炭", "能源", "油气"]):
        terms.extend(["油价", "煤价", "天然气", "OPEC", "地缘风险", "供给扰动"])
    if _contains_any(text, ["创新药", "医药", "CRO", "CXO", "临床"]):
        terms.extend(["临床", "ASCO", "ESMO", "license-out", "首付款", "里程碑"])
    if _contains_any(text, ["恒生科技", "港股科技", "平台", "互联网"]):
        terms.extend(["南下资金", "AI应用", "平台竞争", "广告", "电商", "云业务"])
    if _contains_any(text, ["传媒", "游戏", "动漫", "AIGC", "影视"]):
        terms.extend(["版号", "暑期档", "春节档", "AIGC", "广告", "内容出海"])
    return _unique_strings(terms)


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


def _catalyst_search_groups(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> List[List[str]]:
    terms = _catalyst_search_terms(metadata, profile)
    name = str(metadata.get("name", "")).strip()
    sector = str(metadata.get("sector", "")).strip()
    chain_nodes = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    groups: List[List[str]] = []
    if name:
        groups.append([name])
        if sector and sector != name:
            groups.append([name, sector])
    if len(chain_nodes) >= 2:
        groups.append(list(chain_nodes[:2]))
    elif chain_nodes:
        groups.append([chain_nodes[0]])
    if sector and terms:
        groups.append([sector, terms[0]])
    if len(terms) >= 2:
        groups.append(list(terms[:2]))
    if len(terms) >= 4:
        groups.append([terms[0], terms[2], terms[3]])
    deduped: List[List[str]] = []
    seen: set[tuple[str, ...]] = set()
    for group in groups:
        cleaned = tuple(str(item).strip() for item in group if str(item).strip())
        if not cleaned or cleaned in seen:
            continue
        seen.add(cleaned)
        deduped.append(list(cleaned))
    return deduped[:6]


HOT_THEME_CATALYST_PROFILES = {
    "科技",
    "半导体",
    "纳斯达克",
    "港股科技",
    "创新药",
    "黄金",
    "有色",
    "能源",
    "电网",
    "军工",
    "消费",
    "白酒",
    "宽基",
}


def _expected_high_newsflow(metadata: Mapping[str, Any], profile: Mapping[str, Any]) -> bool:
    profile_name = str(profile.get("profile_name", "")).strip()
    if profile_name in HOT_THEME_CATALYST_PROFILES:
        return True
    text = " ".join(
        [
            str(metadata.get("name", "")),
            str(metadata.get("sector", "")),
            str(metadata.get("industry", "")),
            " ".join(str(item) for item in metadata.get("chain_nodes", []) or []),
        ]
    )
    return _contains_any(
        text,
        [
            "半导体",
            "芯片",
            "算力",
            "创新药",
            "港股科技",
            "黄金",
            "有色",
            "煤炭",
            "电网",
            "白酒",
            "红利",
            "沪深300",
            "中证A500",
            "中证500",
            "创业板",
        ],
    )


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
    asset_type = str(metadata.get("asset_type", "")).strip()
    profile_name = str(profile.get("profile_name", "")).strip()
    if region == "US" or profile_name == "纳斯达克":
        return ["Reuters", "Investor Relations", "SEC", "Bloomberg", "Financial Times"]
    if region == "HK" or profile_name == "港股科技":
        return ["Reuters", "HKEXnews", "Investor Relations", "Bloomberg", "Financial Times"]
    if asset_type == "cn_stock":
        return ["CNINFO", "SSE", "SZSE", "Investor Relations", "财联社", "证券时报", "Reuters", "Bloomberg"]
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
    category = str(item.get("category", "")).strip().lower()
    if category in {"stock_announcement", "repurchase", "dividend"}:
        return True
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
            "cninfo.com.cn",
            "sse.com.cn",
            "szse.cn",
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


def _instrument_identity_tokens(metadata: Mapping[str, Any]) -> List[str]:
    tokens: List[str] = []
    for key in ("name", "display_name", "full_name", "fund_name"):
        value = str(metadata.get(key, "")).strip()
        if value:
            tokens.append(value)
    symbol = str(metadata.get("symbol", "")).strip()
    if symbol:
        tokens.extend([symbol, symbol.upper()])
    return list(dict.fromkeys([token for token in tokens if token]))


def _instrument_specific_news_items(
    items: Sequence[Mapping[str, Any]],
    metadata: Mapping[str, Any],
) -> List[Mapping[str, Any]]:
    identity_tokens = _instrument_identity_tokens(metadata)
    if not identity_tokens:
        return []
    return [
        item
        for item in items
        if _contains_any(_headline_text(item), identity_tokens)
    ]


def _context_now(context: Mapping[str, Any]) -> datetime:
    for key in ("as_of", "now"):
        candidate = context.get(key)
        if isinstance(candidate, datetime):
            return candidate
        if candidate not in (None, ""):
            parsed = pd.to_datetime(candidate, errors="coerce")
            if not pd.isna(parsed):
                return parsed.to_pydatetime()
    return datetime.now()


def _runtime_feature_disabled(context: Mapping[str, Any], flag: str) -> bool:
    return bool(dict(context.get("config") or {}).get(flag, False))


def _is_disclosure_like_item(item: Mapping[str, Any], stock_name_tokens: Sequence[str] = ()) -> bool:
    text = _headline_text(item)
    if stock_name_tokens and not _contains_any(text, stock_name_tokens):
        return False
    if _contains_any(text, DISCLOSURE_WINDOW_KEYS):
        return True
    has_period_marker = bool(re.search(r"20\d{2}年", text)) or _contains_any(text, DISCLOSURE_PERIOD_KEYS)
    has_result_marker = _contains_any(text, DISCLOSURE_RESULT_KEYS)
    return has_period_marker and has_result_marker


def _is_ir_interaction_item(item: Mapping[str, Any]) -> bool:
    blob = " ".join(
        [
            str(item.get("title", "")),
            str(item.get("note", "")),
            str(item.get("lead_detail", "")),
            str(item.get("configured_source", "")),
            str(item.get("source", "")),
        ]
    ).lower()
    return _contains_any(blob, IR_INTERACTION_KEYS)


def _is_structured_company_event_item(item: Mapping[str, Any], stock_name_tokens: Sequence[str] = ()) -> bool:
    category = str(item.get("category", "")).strip().lower()
    if category == "earnings_calendar":
        return True
    text = _headline_text(item)
    if stock_name_tokens and category != "stock_announcement" and not _contains_any(text, stock_name_tokens):
        return False
    if _is_disclosure_like_item(item, stock_name_tokens if category != "stock_announcement" else ()):
        return True
    if _is_ir_interaction_item(item):
        return True
    if category == "stock_announcement" and _contains_any(text, STRUCTURED_COMPANY_EVENT_KEYS):
        return True
    if _is_high_confidence_company_news(item) and _contains_any(text, STRUCTURED_COMPANY_EVENT_KEYS):
        return True
    return False


def _is_non_positive_company_statement(item: Mapping[str, Any]) -> bool:
    return _contains_any(
        _headline_text(item),
        [*NON_POSITIVE_COMPANY_STATEMENT_KEYS, *NEGATIVE_DILUTION_KEYS, *NEGATIVE_REGULATORY_KEYS],
    )


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
    cache = _runtime_cache_bucket(context, "stock_holdertrade_snapshot")
    cache_key = f"{symbol}:{lookback_days}"
    if cache_key in cache:
        return dict(cache.get(cache_key) or {})
    collector = ValuationCollector(dict(context.get("config", {})))
    reference = _context_now(context)
    display_name = str(metadata.get("name", symbol)).strip() or symbol
    try:
        rows = collector.get_cn_stock_holder_trades(symbol)
    except Exception:
        rows = []
    if not rows:
        cache[cache_key] = {}
        return {}
    frame = pd.DataFrame(rows)
    if frame.empty or "ann_date" not in frame.columns:
        cache[cache_key] = {}
        return {}
    frame["ann_date"] = pd.to_datetime(frame["ann_date"], errors="coerce")
    frame = frame.dropna(subset=["ann_date"])
    if frame.empty:
        cache[cache_key] = {}
        return {}
    frame = frame[(reference - frame["ann_date"]).dt.days.between(0, lookback_days)]
    if frame.empty:
        cache[cache_key] = {}
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
        cache[cache_key] = {}
        return {}
    if increase_ratio > decrease_ratio:
        net_ratio = round(increase_ratio - decrease_ratio, 4)
        title = f"{display_name} 近 {lookback_days} 日高管/股东净增持约 {net_ratio:.2f}%"
        direction = "increase"
    else:
        net_ratio = round(decrease_ratio - increase_ratio, 4)
        title = f"{display_name} 近 {lookback_days} 日高管/股东净减持约 {net_ratio:.2f}%"
        direction = "decrease"
    payload = {
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
    cache[cache_key] = payload
    return dict(payload)


def _cn_holder_concentration_snapshot(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    collector = ValuationCollector(dict(context.get("config", {})))
    try:
        total_rows = collector.get_cn_stock_top10_holders(symbol)
    except Exception:
        total_rows = []
    try:
        float_rows = collector.get_cn_stock_top10_floatholders(symbol)
    except Exception:
        float_rows = []

    def _latest_block(rows: Sequence[Mapping[str, Any]], ratio_key: str) -> tuple[float, str, int]:
        if not rows:
            return 0.0, "", 0
        frame = pd.DataFrame(rows)
        if frame.empty or "end_date" not in frame.columns:
            return 0.0, "", 0
        frame["end_date"] = pd.to_datetime(frame["end_date"], errors="coerce")
        frame = frame.dropna(subset=["end_date"])
        if frame.empty:
            return 0.0, "", 0
        latest_date = frame["end_date"].max()
        latest = frame[frame["end_date"] == latest_date].copy()
        ratio_series = pd.to_numeric(latest.get(ratio_key), errors="coerce").fillna(0.0)
        return float(ratio_series.sum()), latest_date.date().isoformat(), len(latest)

    total_ratio, total_end_date, total_count = _latest_block(total_rows, "hold_ratio")
    float_ratio, float_end_date, float_count = _latest_block(float_rows, "hold_float_ratio")
    latest_date = total_end_date or float_end_date
    if latest_date == "":
        return {}

    display_name = str(metadata.get("name", symbol)).strip() or symbol
    title = (
        f"{display_name} 最新前十大股东合计约 {total_ratio:.1f}%，"
        f"前十大流通股东合计约 {float_ratio:.1f}%"
    )
    detail = (
        f"最近披露期 {latest_date}；前十大股东 {total_count} 个席位、"
        f"前十大流通股东 {float_count} 个席位。该项只作为筹码稳定性辅助，不直接等同于机构加仓。"
    )
    return {
        "total_ratio": round(total_ratio, 4),
        "float_ratio": round(float_ratio, 4),
        "report_date": latest_date,
        "title": title,
        "detail": detail,
    }


def _cn_pledge_risk_snapshot(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    asset_type = str(metadata.get("asset_type", "")).strip()
    if asset_type and asset_type != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_pledge_risk_runtime"):
        return {
            "status": "ℹ️",
            "detail": "股权质押专题在 discovery 预筛阶段已跳过；如进入正式候选，会补完整 pledge_stat / pledge_detail 校验。",
            "fallback": "runtime_skip",
            "disclosure": "股权质押专题在预筛阶段已跳过，本轮不把缺口误写成质押风险通过。",
            "pledge_ratio": 0.0,
            "active_holder_ratio": 0.0,
            "active_count": 0,
        }
    collector = ValuationCollector(dict(context.get("config", {})))
    try:
        stat_rows = collector.get_cn_stock_pledge_stat(symbol)
    except Exception:
        stat_rows = []
    try:
        detail_rows = collector.get_cn_stock_pledge_detail(symbol)
    except Exception:
        detail_rows = []

    pledge_ratio = 0.0
    end_date = ""
    if stat_rows:
        stat_frame = pd.DataFrame(stat_rows)
        if not stat_frame.empty and "end_date" in stat_frame.columns:
            stat_frame["end_date"] = pd.to_datetime(stat_frame["end_date"], errors="coerce")
            stat_frame = stat_frame.dropna(subset=["end_date"]).sort_values("end_date", ascending=False)
            if not stat_frame.empty:
                latest = stat_frame.iloc[0]
                pledge_ratio = float(pd.to_numeric(pd.Series([latest.get("pledge_ratio")]), errors="coerce").fillna(0.0).iloc[0])
                end_date = latest["end_date"].date().isoformat()

    active_holder_ratio = 0.0
    active_count = 0
    if detail_rows:
        detail_frame = pd.DataFrame(detail_rows)
        if not detail_frame.empty:
            if "is_release" in detail_frame.columns:
                active_mask = detail_frame["is_release"].astype(str) != "1"
                active_frame = detail_frame[active_mask].copy()
            else:
                active_frame = detail_frame.copy()
            if not active_frame.empty:
                active_count = int(len(active_frame))
                active_holder_ratio = float(pd.to_numeric(active_frame.get("h_total_ratio"), errors="coerce").fillna(0.0).max())

    if pledge_ratio >= 15.0 or (pledge_ratio >= 5.0 and active_holder_ratio >= 70.0):
        status = "❌"
    elif pledge_ratio >= 5.0 or active_holder_ratio >= 50.0:
        status = "⚠️"
    elif stat_rows or detail_rows:
        status = "✅"
    else:
        status = "ℹ️"

    if status == "ℹ️":
        detail = "Tushare pledge_stat / pledge_detail 当前不可用，质押风险暂未纳入本轮硬检查。"
    else:
        date_text = end_date or "最近一期"
        detail = f"{date_text} 质押比例约 {pledge_ratio:.2f}%"
        if active_count:
            detail += f"，仍有 {active_count} 条未释放质押，单一股东最高质押占其持股约 {active_holder_ratio:.1f}%"
        else:
            detail += "，当前未见明显未释放质押明细"
    return {
        "status": status,
        "detail": detail,
        "pledge_ratio": round(pledge_ratio, 4),
        "active_holder_ratio": round(active_holder_ratio, 4),
        "active_count": active_count,
    }


def _cn_stock_unlock_pressure_snapshot(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    asset_type = str(metadata.get("asset_type", "")).strip()
    if asset_type and asset_type != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "") or metadata.get("code", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_unlock_pressure_runtime"):
        return {
            "status": "ℹ️",
            "detail": "限售解禁专题在 discovery 预筛阶段已跳过；如进入正式候选，会补完整 share_float 校验。",
            "fallback": "runtime_skip",
            "disclosure": "限售解禁专题在预筛阶段已跳过，本轮不把缺口误写成解禁压力通过。",
            "source": "tushare.share_float",
        }
    cache = _runtime_cache_bucket(context, "stock_unlock_pressure_snapshot")
    if symbol not in cache:
        try:
            cache[symbol] = ChinaMarketCollector(dict(context.get("config", {}))).get_unlock_pressure(symbol)
        except Exception:
            cache[symbol] = {
                "status": "ℹ️",
                "detail": "Tushare share_float 当前不可用，解禁压力暂未纳入本轮检查",
                "fallback": "none",
                "source": "tushare.share_float",
            }
    return dict(cache.get(symbol) or {})


def _cn_stock_regulatory_risk_snapshot(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_regulatory_risk_runtime"):
        return {
            "status": "ℹ️",
            "detail": "交易所风险专题在 discovery 预筛阶段已跳过；如进入正式候选，会补完整 ST/异常波动/重点提示校验。",
            "components": {},
            "active_st": False,
            "high_shock_count": 0,
            "alert_count": 0,
            "active_alert_count": 0,
            "fallback": "runtime_skip",
            "disclosure": "交易所风险专题在预筛阶段已跳过，本轮不把缺口误写成通过。",
        }
    cache = _runtime_cache_bucket(context, "stock_regulatory_risk")
    if symbol not in cache:
        collector = ChinaMarketCollector(dict(context.get("config", {})))
        try:
            cache[symbol] = collector.get_stock_regulatory_risk_snapshot(
                symbol,
                as_of=_context_now(context).strftime("%Y-%m-%d"),
                display_name=str(metadata.get("name", "")).strip(),
            )
        except Exception as exc:
            cache[symbol] = {
                "status": "ℹ️",
                "detail": _client_safe_issue("股票风险专题当前不可用，本轮按缺失处理", exc),
                "components": {},
                "active_st": False,
                "high_shock_count": 0,
                "alert_count": 0,
                "active_alert_count": 0,
                "fallback": "none",
                "disclosure": "股票风险专题当前不可用，本轮不把缺口误写成通过。",
            }
    return dict(cache.get(symbol) or {})


def _cn_stock_theme_membership_snapshot(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    cache = _runtime_cache_bucket(context, "stock_theme_membership")
    if symbol not in cache:
        collector = MarketDriversCollector(dict(context.get("config", {})))
        try:
            cache[symbol] = collector.get_stock_theme_membership(
                symbol,
                reference_date=_context_now(context),
            )
        except Exception as exc:
            cache[symbol] = {
                "status": "blocked",
                "diagnosis": "fetch_error",
                "items": [],
                "disclosure": f"同花顺主题成员当前不可用，本轮按缺失处理：{exc}",
                "fallback": "none",
                "is_fresh": False,
            }
    return dict(cache.get(symbol) or {})


def _cn_stock_chip_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    history: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_chip_snapshot_runtime"):
        return {
            "status": "skipped",
            "diagnosis": "runtime_skip",
            "detail": "真实筹码分布在 discovery 预筛阶段已跳过；如进入正式候选，会补完整 cyq_perf / cyq_chips。",
            "disclosure": "真实筹码分布在预筛阶段已跳过，本轮不把缺口误写成承接确认。",
            "fallback": "runtime_skip",
            "is_fresh": False,
            "source": "tushare.cyq_perf+tushare.cyq_chips",
        }
    current_price = None
    if history is not None and not history.empty and "close" in history.columns:
        try:
            current_price = float(pd.to_numeric(history["close"], errors="coerce").dropna().iloc[-1])
        except Exception:
            current_price = None
    cache = _runtime_cache_bucket(context, "stock_chip_snapshot")
    key = (symbol, round(current_price, 4) if current_price is not None else None)
    if key not in cache:
        collector = ValuationCollector(dict(context.get("config", {})))
        try:
            cache[key] = collector.get_cn_stock_chip_snapshot(
                symbol,
                as_of=_context_now(context).strftime("%Y-%m-%d"),
                current_price=current_price,
            )
        except Exception as exc:
            cache[key] = {
                "status": "blocked",
                "diagnosis": "fetch_error",
                "detail": _client_safe_issue("真实筹码分布当前不可用，本轮按缺失处理", exc),
                "disclosure": "真实筹码分布当前不可用，本轮不把缺口误写成承接确认。",
                "fallback": "none",
                "is_fresh": False,
                "source": "tushare.cyq_perf+tushare.cyq_chips",
            }
    return dict(cache.get(key) or {})


def _cn_stock_capital_flow_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_capital_flow_runtime"):
        return {
            "status": "skipped",
            "diagnosis": "runtime_skip",
            "detail": "个股/行业/概念资金流在 discovery 预筛阶段已跳过；如进入正式候选，会补完整承接链。",
            "disclosure": "个股/行业/概念资金流在预筛阶段已跳过，本轮不把缺口误写成主力承接已确认。",
            "fallback": "runtime_skip",
            "is_fresh": False,
            "source": "tushare.moneyflow+tushare.ths_member+tushare.moneyflow_ind_ths+tushare.moneyflow_cnt_ths",
        }
    cache = _runtime_cache_bucket(context, "stock_capital_flow")
    if symbol not in cache:
        collector = MarketDriversCollector(dict(context.get("config", {})))
        try:
            cache[symbol] = collector.get_stock_capital_flow_snapshot(
                symbol,
                reference_date=_context_now(context),
                display_name=str(metadata.get("name", "")).strip(),
                sector=str(metadata.get("sector", "")).strip(),
                chain_nodes=[str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()],
            )
        except Exception as exc:
            cache[symbol] = {
                "status": "blocked",
                "diagnosis": "fetch_error",
                "detail": _client_safe_issue("个股/行业/概念资金流当前不可用，本轮按缺失处理", exc),
                "disclosure": "个股/行业/概念资金流当前不可用，本轮不把缺口误写成主力承接已经确认。",
                "fallback": "none",
                "is_fresh": False,
                "source": "tushare.moneyflow+tushare.ths_member+tushare.moneyflow_ind_ths+tushare.moneyflow_cnt_ths",
            }
    return dict(cache.get(symbol) or {})


def _cn_stock_broker_recommend_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_broker_recommend_runtime"):
        return {
            "status": "skipped",
            "diagnosis": "runtime_skip",
            "detail": "券商月度金股专题在 discovery 预筛阶段已跳过；如进入正式候选，会补完整卖方覆盖快照。",
            "disclosure": "券商月度金股专题在预筛阶段已跳过，本轮不把缺口误写成零覆盖或低拥挤。",
            "fallback": "runtime_skip",
            "is_fresh": False,
            "source": "tushare.broker_recommend",
            "latest_broker_count": 0,
            "crowding_level": "",
        }
    cache = _runtime_cache_bucket(context, "stock_broker_recommend_snapshot")
    if symbol not in cache:
        collector = MarketDriversCollector(dict(context.get("config", {})))
        try:
            cache[symbol] = collector.get_stock_broker_recommend_snapshot(
                symbol,
                reference_date=_context_now(context),
                display_name=str(metadata.get("name", "")).strip(),
            )
        except Exception as exc:
            cache[symbol] = {
                "status": "blocked",
                "diagnosis": "fetch_error",
                "detail": _client_safe_issue("券商月度金股专题当前不可用，本轮按缺失处理", exc),
                "disclosure": "券商月度金股专题当前不可用，本轮不把缺口误写成零覆盖或低拥挤。",
                "fallback": "none",
                "is_fresh": False,
                "source": "tushare.broker_recommend",
                "latest_broker_count": 0,
                "crowding_level": "",
        }
    return dict(cache.get(symbol) or {})


def _cn_stock_ah_comparison_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    asset_type = str(metadata.get("asset_type", "")).strip()
    if asset_type not in {"cn_stock", "hk", "hk_index"}:
        return {}
    symbol = str(metadata.get("symbol", "") or metadata.get("code", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_ah_comparison_runtime"):
        return {
            "status": "skipped",
            "diagnosis": "runtime_skip",
            "detail": "A/H 比价在 discovery 预筛阶段已跳过；如进入正式候选，会补完整 cross-market comparison。",
            "disclosure": "A/H 比价在预筛阶段已跳过，本轮不把缺口误写成跨市场比价已确认。",
            "fallback": "runtime_skip",
            "is_fresh": False,
            "source": "tushare.stk_ah_comparison",
        }
    cache = _runtime_cache_bucket(context, "stock_ah_comparison")
    key = (asset_type, symbol)
    if key not in cache:
        collector = ChinaMarketCollector(dict(context.get("config", {})))
        try:
            trade_date = _context_now(context).strftime("%Y-%m-%d")
            if asset_type == "cn_stock":
                frame = collector.get_stk_ah_comparison(ts_code=symbol, trade_date=trade_date)
            else:
                frame = collector.get_stk_ah_comparison(hk_code=symbol, trade_date=trade_date)
            cache[key] = _summarize_ah_comparison_snapshot(frame, asset_type=asset_type, symbol=symbol)
        except Exception as exc:
            cache[key] = {
                "status": "blocked",
                "diagnosis": "fetch_error",
                "detail": _client_safe_issue("A/H 比价当前不可用，本轮按缺失处理", exc),
                "disclosure": "A/H 比价当前不可用，本轮不把缺口误写成跨市场比价已确认。",
                "fallback": "none",
                "is_fresh": False,
                "source": "tushare.stk_ah_comparison",
            }
    return dict(cache.get(key) or {})


def _summarize_ah_comparison_snapshot(
    frame: pd.DataFrame | None,
    *,
    asset_type: str,
    symbol: str,
) -> Dict[str, Any]:
    if frame is None or frame.empty:
        return {
            "status": "empty",
            "diagnosis": "empty",
            "detail": "Tushare stk_ah_comparison 当前返回空表，本轮按缺失处理，不伪装成 fresh。",
            "disclosure": "A/H 比价空表，本轮按缺失处理。",
            "fallback": "none",
            "is_fresh": False,
            "latest_date": "",
            "source": "tushare.stk_ah_comparison",
            "row": {},
        }
    working = frame.copy()
    if "trade_date" in working.columns:
        working["trade_date"] = working["trade_date"].map(BaseCollector._normalize_date_text)
        working = working.sort_values("trade_date", ascending=False, na_position="last").reset_index(drop=True)
    row = dict(working.iloc[0].to_dict())
    latest_date = str(row.get("trade_date", "") or "").strip()
    is_fresh = bool(latest_date)
    premium_rate = None
    for key in ("premium_rate", "ah_premium", "ah_comparison"):
        value = row.get(key)
        if value is None:
            continue
        try:
            premium_rate = float(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0])
            if premium_rate == premium_rate:
                break
        except (TypeError, ValueError):
            continue
    if premium_rate is None:
        ratio_value = None
        for key in ("comparison_ratio", "ah_ratio", "ratio"):
            value = row.get(key)
            if value is None:
                continue
            try:
                ratio_value = float(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0])
                if ratio_value == ratio_value:
                    break
            except (TypeError, ValueError):
                continue
        if ratio_value is not None and ratio_value > 0:
            premium_rate = (ratio_value - 1.0) * 100.0
    detail_parts = [f"{symbol} A/H 比价快照"]
    a_name = str(row.get("a_name") or row.get("name") or "").strip()
    hk_name = str(row.get("hk_name") or "").strip()
    if a_name or hk_name:
        detail_parts.append(f"{a_name or symbol} vs {hk_name or 'HK'}")
    if premium_rate is not None:
        detail_parts.append(f"溢价/折价约 {premium_rate:+.2f}%")
    source_hint = "tushare.stk_ah_comparison"
    disclosure = "Tushare stk_ah_comparison 提供 AH 股比价快照；空表或受限时按缺失处理，不伪装成 fresh。"
    if not is_fresh:
        disclosure = "Tushare stk_ah_comparison 当前未拿到明确交易日期，本轮不把缺口误写成 fresh。"
    return {
        "status": "matched",
        "diagnosis": "live" if is_fresh else "stale",
        "detail": "；".join(detail_parts),
        "disclosure": disclosure,
        "fallback": "none" if is_fresh else "stale",
        "is_fresh": is_fresh,
        "latest_date": latest_date,
        "source": source_hint,
        "asset_type": asset_type,
        "symbol": symbol,
        "row": row,
        "premium_rate": premium_rate,
    }


def _cn_stock_convertible_bond_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")).strip() != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "") or metadata.get("code", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_convertible_bond_runtime"):
        return {
            "status": "skipped",
            "diagnosis": "runtime_skip",
            "detail": "可转债专题在 discovery 预筛阶段已跳过；如进入正式候选，会补完整 cb_basic/cb_daily/cb_factor_pro。",
            "disclosure": "可转债专题在预筛阶段已跳过，本轮不把缺口误写成可转债映射已确认。",
            "fallback": "runtime_skip",
            "is_fresh": False,
            "source": "tushare.cb_basic+tushare.cb_daily+tushare.cb_factor_pro",
        }
    cache = _runtime_cache_bucket(context, "stock_convertible_bond_snapshot")
    if symbol not in cache:
        collector = ChinaMarketCollector(dict(context.get("config", {})))
        try:
            exchange = "SH" if symbol[:1] in {"5", "6", "9"} else "SZ"
            basic = collector.get_cb_basic(exchange=exchange)
            if basic is None or basic.empty:
                basic = collector.get_cb_basic(exchange="SZ" if exchange == "SH" else "SH")
            cache[symbol] = _summarize_convertible_bond_snapshot(
                basic,
                collector=collector,
                symbol=symbol,
                name=str(metadata.get("name", "")).strip(),
                as_of=_context_now(context).strftime("%Y-%m-%d"),
            )
        except Exception as exc:
            cache[symbol] = {
                "status": "blocked",
                "diagnosis": "fetch_error",
                "detail": _client_safe_issue("可转债专题当前不可用，本轮按缺失处理", exc),
                "disclosure": "可转债专题当前不可用，本轮不把缺口误写成可转债映射已确认。",
                "fallback": "none",
                "is_fresh": False,
                "source": "tushare.cb_basic+tushare.cb_daily+tushare.cb_factor_pro",
            }
    return dict(cache.get(symbol) or {})


def _summarize_convertible_bond_snapshot(
    basic_frame: pd.DataFrame | None,
    *,
    collector: ChinaMarketCollector,
    symbol: str,
    name: str,
    as_of: str,
) -> Dict[str, Any]:
    if basic_frame is None or basic_frame.empty:
        return {
            "status": "empty",
            "diagnosis": "empty",
            "detail": "Tushare cb_basic 当前返回空表，本轮按缺失处理，不伪装成 fresh。",
            "disclosure": "可转债基础信息空表，本轮按缺失处理。",
            "fallback": "none",
            "is_fresh": False,
            "latest_date": "",
            "source": "tushare.cb_basic+tushare.cb_daily+tushare.cb_factor_pro",
            "row": {},
        }
    working = basic_frame.copy()
    for column in ("list_date", "delist_date", "value_date", "maturity_date", "conv_start_date", "conv_end_date", "conv_stop_date"):
        if column in working.columns:
            working[column] = working[column].map(BaseCollector._normalize_date_text)
    match_frame = working
    if "stk_code" in match_frame.columns:
        match_frame = match_frame[match_frame["stk_code"].astype(str).str.strip() == symbol]
    if match_frame.empty and "stk_short_name" in working.columns and name:
        match_frame = working[working["stk_short_name"].astype(str).str.strip().str.contains(name, na=False)]
    if match_frame.empty:
        return {
            "status": "empty",
            "diagnosis": "no_match",
            "detail": f"可转债基础信息已接入，但未找到与 {symbol} 匹配的发行人。",
            "disclosure": "可转债基础信息已接入，但当前标的未命中对应转债发行人。",
            "fallback": "none",
            "is_fresh": False,
            "latest_date": "",
            "source": "tushare.cb_basic+tushare.cb_daily+tushare.cb_factor_pro",
            "row": {},
        }

    match_frame = match_frame.sort_values(
        [column for column in ("list_date", "ts_code") if column in match_frame.columns],
        ascending=[False] * len([column for column in ("list_date", "ts_code") if column in match_frame.columns]),
        na_position="last",
    ).reset_index(drop=True)
    row = dict(match_frame.iloc[0].to_dict())
    bond_code = str(row.get("ts_code", "")).strip()
    if not bond_code:
        return {
            "status": "empty",
            "diagnosis": "no_bond_code",
            "detail": f"可转债基础信息已命中发行人，但未找到可用转债代码。",
            "disclosure": "可转债基础信息命中发行人，但缺少转债代码。",
            "fallback": "none",
            "is_fresh": False,
            "latest_date": "",
            "source": "tushare.cb_basic+tushare.cb_daily+tushare.cb_factor_pro",
            "row": row,
        }

    trade_date = as_of.replace("-", "")
    try:
        daily = collector.get_cb_daily(ts_code=bond_code, trade_date=trade_date)
    except Exception:
        daily = pd.DataFrame()
    try:
        factor = collector.get_cb_factor_pro(ts_code=bond_code, trade_date=trade_date)
    except Exception:
        factor = pd.DataFrame()

    daily_row = dict(daily.iloc[0].to_dict()) if isinstance(daily, pd.DataFrame) and not daily.empty else {}
    factor_row = dict(factor.iloc[0].to_dict()) if isinstance(factor, pd.DataFrame) and not factor.empty else {}
    latest_date = str(daily_row.get("trade_date") or factor_row.get("trade_date") or row.get("list_date") or "").strip()
    is_fresh = bool(latest_date)

    premium_rate = None
    for key in ("cb_over_rate", "bond_over_rate", "premium_rate"):
        value = daily_row.get(key)
        if value is None:
            continue
        try:
            premium_rate = float(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0])
            if premium_rate == premium_rate:
                break
        except (TypeError, ValueError):
            continue

    close_value = None
    for key in ("close", "bond_close", "latest_price"):
        value = daily_row.get(key)
        if value is None:
            continue
        try:
            close_value = float(pd.to_numeric(pd.Series([value]), errors="coerce").iloc[0])
            if close_value == close_value:
                break
        except (TypeError, ValueError):
            continue

    remain_size = pd.to_numeric(pd.Series([row.get("remain_size")]), errors="coerce").iloc[0]
    remain_size_yi = float(remain_size) / 1e8 if remain_size is not None and remain_size == remain_size else None

    ma5 = pd.to_numeric(pd.Series([factor_row.get("ma_bfq_5")]), errors="coerce").iloc[0]
    ma20 = pd.to_numeric(pd.Series([factor_row.get("ma_bfq_20")]), errors="coerce").iloc[0]
    macd_dif = pd.to_numeric(pd.Series([factor_row.get("macd_dif_bfq")]), errors="coerce").iloc[0]
    macd_dea = pd.to_numeric(pd.Series([factor_row.get("macd_dea_bfq")]), errors="coerce").iloc[0]
    rsi6 = pd.to_numeric(pd.Series([factor_row.get("rsi_bfq_6")]), errors="coerce").iloc[0]

    trend_label = "震荡"
    if close_value is not None and ma5 == ma5 and ma20 == ma20:
        if close_value > float(ma5) > float(ma20):
            trend_label = "趋势偏强"
        elif close_value < float(ma5) < float(ma20):
            trend_label = "趋势偏弱"
    elif macd_dif == macd_dif and macd_dea == macd_dea:
        if float(macd_dif) > float(macd_dea):
            trend_label = "趋势偏强"
        elif float(macd_dif) < float(macd_dea):
            trend_label = "趋势偏弱"

    momentum_label = "动能中性"
    if rsi6 == rsi6:
        if float(rsi6) >= 60:
            momentum_label = "动能改善"
        elif float(rsi6) <= 40:
            momentum_label = "动能偏弱"
    elif daily_row.get("pct_chg") is not None:
        pct_chg = pd.to_numeric(pd.Series([daily_row.get("pct_chg")]), errors="coerce").iloc[0]
        if pct_chg == pct_chg:
            momentum_label = "动能改善" if float(pct_chg) > 0 else "动能偏弱" if float(pct_chg) < 0 else "动能中性"

    detail_parts = [
        f"{row.get('stk_short_name') or name or symbol} 对应转债 {row.get('bond_short_name') or bond_code}",
        f"转债 {trend_label}",
        f"{momentum_label}",
    ]
    if premium_rate is not None:
        detail_parts.append(f"转股溢价约 {premium_rate:+.2f}%")
    if remain_size_yi is not None:
        detail_parts.append(f"余额约 {remain_size_yi:.1f} 亿")

    disclosure = "Tushare cb_basic / cb_daily / cb_factor_pro 已接入；空表、未匹配或非当期时不伪装成 fresh。"
    if not is_fresh:
        disclosure = "Tushare 可转债快照未拿到明确最新日期，本轮不把缺口误写成 fresh。"
    return {
        "status": "matched",
        "diagnosis": "live" if is_fresh else "stale",
        "detail": "；".join(detail_parts),
        "disclosure": disclosure,
        "fallback": "none" if is_fresh else "stale",
        "is_fresh": is_fresh,
        "latest_date": latest_date,
        "source": "tushare.cb_basic+tushare.cb_daily+tushare.cb_factor_pro",
        "symbol": symbol,
        "bond_code": bond_code,
        "row": row,
        "daily_row": daily_row,
        "factor_row": factor_row,
        "premium_rate": premium_rate,
        "trend_label": trend_label,
        "momentum_label": momentum_label,
        "remain_size_yi": remain_size_yi,
        "close": close_value,
    }


def _cn_stock_margin_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_margin_runtime"):
        return {
            "status": "ℹ️",
            "diagnosis": "runtime_skip",
            "detail": "个股两融明细在 discovery 预筛阶段已跳过；如进入正式候选，会补完整拥挤度判断。",
            "disclosure": "个股两融明细在预筛阶段已跳过，本轮不把缺口误写成融资盘已经退潮。",
            "fallback": "runtime_skip",
            "is_fresh": False,
            "source": "tushare.margin_detail",
        }
    cache = _runtime_cache_bucket(context, "stock_margin_snapshot")
    if symbol not in cache:
        collector = ChinaMarketCollector(dict(context.get("config", {})))
        try:
            cache[symbol] = collector.get_stock_margin_snapshot(
                symbol,
                as_of=_context_now(context).strftime("%Y-%m-%d"),
                display_name=str(metadata.get("name", "")).strip(),
            )
        except Exception as exc:
            cache[symbol] = {
                "status": "ℹ️",
                "diagnosis": "fetch_error",
                "detail": _client_safe_issue("两融明细当前不可用，本轮按缺失处理", exc),
                "disclosure": "个股两融明细当前不可用，本轮不把缺口误写成融资盘已经退潮。",
                "fallback": "none",
                "is_fresh": False,
                "source": "tushare.margin_detail",
            }
    return dict(cache.get(symbol) or {})


def _cn_stock_board_action_snapshot(
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    history: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    if str(metadata.get("asset_type", "")) != "cn_stock":
        return {}
    symbol = str(metadata.get("symbol", "")).strip()
    if not symbol:
        return {}
    if _runtime_feature_disabled(context, "skip_cn_stock_board_action_runtime"):
        return {
            "status": "ℹ️",
            "detail": "龙虎榜/竞价/涨跌停专题在 discovery 预筛阶段已跳过；如进入正式候选，会补完整微观交易结构。",
            "disclosure": "打板专题在预筛阶段已跳过，本轮不把缺口误写成没有龙虎榜或情绪风险。",
            "fallback": "runtime_skip",
            "is_fresh": False,
            "source": "tushare.top_list+tushare.top_inst+tushare.stk_auction+tushare.stk_limit+tushare.limit_list_d",
        }
    current_price = None
    if history is not None and not history.empty and "close" in history.columns:
        try:
            current_price = float(pd.to_numeric(history["close"], errors="coerce").dropna().iloc[-1])
        except Exception:
            current_price = None
    cache = _runtime_cache_bucket(context, "stock_board_action_snapshot")
    key = (symbol, round(current_price, 4) if current_price is not None else None)
    if key not in cache:
        collector = MarketPulseCollector(dict(context.get("config", {})))
        try:
            cache[key] = collector.get_stock_board_action_snapshot(
                symbol,
                reference_date=_context_now(context),
                display_name=str(metadata.get("name", "")).strip(),
                current_price=current_price,
            )
        except Exception as exc:
            cache[key] = {
                "status": "ℹ️",
                "detail": _client_safe_issue("龙虎榜/竞价/涨跌停专题当前不可用，本轮按缺失处理", exc),
                "disclosure": "打板专题当前不可用，本轮不把缺口误写成没有龙虎榜或情绪风险。",
                "fallback": "none",
                "is_fresh": False,
                "source": "tushare.top_list+tushare.top_inst+tushare.stk_auction+tushare.stk_limit+tushare.limit_list_d",
            }
    return dict(cache.get(key) or {})


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
        cash_div = pd.to_numeric(pd.Series([row.get("cash_div")]), errors="coerce").iloc[0]
        cash_div_tax = pd.to_numeric(pd.Series([row.get("cash_div_tax")]), errors="coerce").iloc[0]
        cash_per_share = cash_div_tax if not pd.isna(cash_div_tax) and float(cash_div_tax) > 0 else cash_div
        ratio_parts: List[str] = []
        if not pd.isna(cash_per_share) and float(cash_per_share) > 0:
            ratio_parts.append(f"每10股派现 {float(cash_per_share) * 10:.2f} 元")
        bonus_share = pd.to_numeric(pd.Series([row.get("stk_bo_rate")]), errors="coerce").iloc[0]
        transfer_share = pd.to_numeric(pd.Series([row.get("stk_co_rate")]), errors="coerce").iloc[0]
        stock_share = pd.to_numeric(pd.Series([row.get("stk_div")]), errors="coerce").iloc[0]
        if not pd.isna(bonus_share) and float(bonus_share) > 0:
            ratio_parts.append(f"每10股送股 {float(bonus_share) * 10:.2f} 股")
        if not pd.isna(transfer_share) and float(transfer_share) > 0:
            ratio_parts.append(f"每10股转增 {float(transfer_share) * 10:.2f} 股")
        elif (
            (pd.isna(bonus_share) or float(bonus_share) <= 0)
            and not pd.isna(stock_share)
            and float(stock_share) > 0
        ):
            ratio_parts.append(f"每10股送转 {float(stock_share) * 10:.2f} 股")
        ratio_text = f"（{'，'.join(ratio_parts)}）" if ratio_parts else ""
        items.append(
            {
                "title": f"{display_name} 披露现金分红{div_proc}{ratio_text}",
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


def _is_fresh_intelligence_item(
    item: Mapping[str, Any],
    reference: datetime,
    *,
    fresh_days: int = CATALYST_FRESH_NEWS_DAYS,
) -> bool:
    event_date = _item_datetime(item, reference)
    if event_date is None:
        freshness_bucket = str(item.get("freshness_bucket", "")).strip().lower()
        if freshness_bucket:
            if freshness_bucket == "unknown":
                return True
            return freshness_bucket in {"fresh", "recent"}
        # A large share of repo test fixtures and structured payloads do not carry
        # explicit publish timestamps. Treat these as usable current intelligence
        # unless another freshness signal explicitly marks them stale.
        return True
    age_days = (reference - event_date).total_seconds() / 86400.0
    return -1 <= age_days <= max(int(fresh_days), 1)


def _is_within_lookback(
    item: Mapping[str, Any],
    reference: datetime,
    *,
    lookback_days: int,
) -> bool:
    event_date = _item_datetime(item, reference)
    if event_date is None:
        return True
    age_days = (reference - event_date).total_seconds() / 86400.0
    return -1 <= age_days <= max(int(lookback_days), 1)


def _filter_fresh_intelligence(
    items: Sequence[Mapping[str, Any]],
    reference: datetime,
    *,
    fresh_days: int = CATALYST_FRESH_NEWS_DAYS,
) -> List[Mapping[str, Any]]:
    return [item for item in items if _is_fresh_intelligence_item(item, reference, fresh_days=fresh_days)]


def _latest_intelligence_at(items: Sequence[Mapping[str, Any]], reference: datetime) -> str:
    stamps = [stamp for stamp in (_item_datetime(item, reference) for item in items) if stamp is not None]
    if not stamps:
        return ""
    return max(stamps).strftime("%Y-%m-%d")


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
    *,
    reference_time: Optional[datetime] = None,
) -> Optional[Mapping[str, Any]]:
    if not items:
        return None
    ref = reference_time or datetime.now()

    def _score(item: Mapping[str, Any]) -> tuple[int, int, int, int, str]:
        text = _headline_text(item)
        primary = sum(1 for keyword in primary_keywords if str(keyword).strip() and str(keyword).lower() in text)
        bonus = sum(1 for keyword in bonus_keywords if str(keyword).strip() and str(keyword).lower() in text)
        direct = 1 if _is_high_confidence_company_news(item) else 0
        freshness = 1 if _is_fresh_intelligence_item(item, ref) else 0
        date_text = _latest_intelligence_at([item], ref)
        return (primary, bonus, direct, freshness, date_text)

    return max(items, key=_score)


def _pick_top_news_items(
    items: Sequence[Mapping[str, Any]],
    primary_keywords: Sequence[str],
    bonus_keywords: Sequence[str],
    *,
    limit: int = 2,
    reference_time: Optional[datetime] = None,
) -> List[Mapping[str, Any]]:
    remaining = [dict(item) for item in items]
    chosen: List[Mapping[str, Any]] = []
    while remaining and len(chosen) < max(int(limit), 1):
        best = _pick_best_news_item(
            remaining,
            primary_keywords,
            bonus_keywords,
            reference_time=reference_time,
        )
        if not best:
            break
        chosen.append(best)
        best_key = (str(best.get("title", "")).strip(), str(best.get("source", "")).strip())
        remaining = [
            item
            for item in remaining
            if (str(item.get("title", "")).strip(), str(item.get("source", "")).strip()) != best_key
        ]
    return chosen


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


def _theme_alignment_tokens_for_label(label: str) -> tuple[str, ...]:
    if "能源" in label or "地缘" in label:
        return ("能源", "油气", "原油", "煤炭", "电网", "黄金", "高股息", "有色", "资源")
    if "利率" in label:
        return ("科技", "信息技术", "电子", "半导体", "消费")
    if "政策" in label:
        return ("电网", "高股息", "消费", "央企", "中字头")
    if "AI" in label or "半导体" in label:
        return ("信息技术", "电子", "半导体", "芯片", "算力", "通信设备", "光模块", "光通信", "服务器", "数据中心", "idc", "交换机", "以太网", "cpo", "人工智能")
    if "通信" in label or "CPO" in label or "光模块" in label:
        return ("通信", "通信设备", "光模块", "光通信", "cpo", "数据中心", "idc", "运营商", "5g", "6g")
    if "游戏" in label or "传媒" in label or "AIGC" in label:
        return ("传媒", "游戏", "动漫", "影视", "aigc", "ai应用", "版号")
    if "创新药" in label or "医药" in label or "BD" in label:
        return ("医药", "创新药", "生物", "制药", "医疗", "cro", "cxo")
    return ()


def _theme_alignment_match(metadata: Mapping[str, Any], day_theme: Mapping[str, Any]) -> tuple[str, str]:
    labels = _day_theme_labels(day_theme)
    if not labels:
        return "", ""

    primary_parts = [
        str(metadata.get("name", "")).strip(),
        str(metadata.get("sector", "")).strip(),
        str(metadata.get("industry", "")).strip(),
        str(metadata.get("industry_framework_label", "")).strip(),
        str(metadata.get("tushare_theme_industry", "")).strip(),
        str(metadata.get("benchmark_name", "")).strip(),
        str(metadata.get("benchmark", "")).strip(),
        str(metadata.get("tracked_index_name", "")).strip(),
        str(metadata.get("index_name", "")).strip(),
        str(metadata.get("index_framework_label", "")).strip(),
    ]
    secondary_parts = [str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip()]
    primary_text = " / ".join(part for part in primary_parts if part)
    secondary_text = " / ".join(part for part in secondary_parts if part)

    def _matches(text: str, tokens: Sequence[str]) -> bool:
        lowered = text.lower()
        return any(str(token).strip().lower() in lowered for token in tokens if str(token).strip())

    for label in labels:
        tokens = _theme_alignment_tokens_for_label(label)
        if tokens and _matches(primary_text, tokens):
            return "direct", label
    for label in labels:
        tokens = _theme_alignment_tokens_for_label(label)
        if tokens and _matches(secondary_text, tokens):
            return "indirect", label
    return "", ""


def _theme_alignment_level(metadata: Mapping[str, Any], day_theme: Mapping[str, Any]) -> str:
    level, _label = _theme_alignment_match(metadata, day_theme)
    return level


def _theme_alignment(metadata: Mapping[str, Any], day_theme: Mapping[str, Any]) -> bool:
    return _theme_alignment_level(metadata, day_theme) == "direct"


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
        regulatory_snapshot = _cn_stock_regulatory_risk_snapshot(metadata, context)
        components = dict(regulatory_snapshot.get("components") or {})
        stock_st_component = dict(components.get("stock_st") or {})
        st_component = dict(components.get("st") or {})
        high_shock_component = dict(components.get("stk_high_shock") or {})
        alert_component = dict(components.get("stk_alert") or {})

        st_detail = str(stock_st_component.get("detail", "")).strip() or str(st_component.get("detail", "")).strip() or "当前未拿到 ST 风险警示板名单"
        st_status = str(stock_st_component.get("status", "")).strip() or str(st_component.get("status", "ℹ️")).strip() or "ℹ️"
        checks.append({"name": "ST 风险", "status": st_status, "detail": st_detail})
        if bool(regulatory_snapshot.get("active_st")):
            exclusion_reasons.append("ST / *ST 股票，退市风险较高")
            warnings.append("⚠️ 当前仍处于 ST / *ST 风险警示板，不能把盘面热度直接写成可执行机会。")

        high_shock_status = str(high_shock_component.get("status", "ℹ️")).strip() or "ℹ️"
        high_shock_detail = str(high_shock_component.get("detail", "当前未拿到严重异常波动记录")).strip() or "当前未拿到严重异常波动记录"
        checks.append({"name": "严重异常波动", "status": high_shock_status, "detail": high_shock_detail})
        if high_shock_status in {"⚠️", "❌"}:
            warnings.append("⚠️ 近窗口命中过交易所严重异常波动，短线波动和情绪反转风险都更高。")

        alert_status = str(alert_component.get("status", "ℹ️")).strip() or "ℹ️"
        alert_detail = str(alert_component.get("detail", "当前未拿到交易所重点提示记录")).strip() or "当前未拿到交易所重点提示记录"
        checks.append({"name": "交易所重点提示", "status": alert_status, "detail": alert_detail})
        if alert_status in {"⚠️", "❌"}:
            warnings.append("⚠️ 当前仍在交易所重点提示证券名单或近窗口曾被重点提示，执行上需要更保守。")

    floor_status, fundamental_floor_detail, floor_exclusion, floor_warning = _fundamental_floor_snapshot(
        asset_type,
        metadata,
        fundamental_dimension,
        fund_profile,
    )
    checks.append({"name": "基本面底线", "status": floor_status, "detail": fundamental_floor_detail})
    if floor_exclusion:
        exclusion_reasons.append(floor_exclusion)
    if floor_warning:
        warnings.append(floor_warning)
    valuation_snapshot = dict(fundamental_dimension.get("valuation_snapshot") or {})
    valuation_extreme = bool(fundamental_dimension.get("valuation_extreme"))
    pe_ttm = valuation_snapshot.get("pe_ttm")
    valuation_label = "个股估值" if asset_type == "cn_stock" else str(valuation_snapshot.get("display_label", "真实指数估值"))
    valuation_match_quality = str(valuation_snapshot.get("match_quality", "")).strip()
    if valuation_extreme and pe_ttm is not None:
        if valuation_match_quality == "theme_proxy":
            checks.append(
                {
                    "name": "估值极端",
                    "status": "⚠️",
                    "detail": (
                        f"{valuation_snapshot.get('index_name', '相关指数')} "
                        f"{valuation_snapshot.get('metric_label', '滚动PE')} {float(pe_ttm):.1f}x，"
                        "当前只作为最接近主题估值代理，提示位置偏贵。"
                    ),
                }
            )
            warnings.append("⚠️ 当前估值代理提示位置偏贵，但因未命中精确基准，只把它当作辅助约束。")
        else:
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
    elif price_percentile >= 0.90:
        checks.append({"name": "估值极端", "status": "⚠️", "detail": f"价格位置代理分位 {price_percentile:.0%}，接近极端高位"})
        warnings.append("⚠️ 价格位置已在高位区，追高性价比明显下降；当前更适合按仓位和确认条件参与，不再直接做硬排除。")
    else:
        checks.append({"name": "估值极端", "status": "✅", "detail": f"价格位置代理分位 {price_percentile:.0%}"})

    if asset_type in {"cn_fund", "cn_etf", "cn_index"}:
        checks.append({"name": "解禁压力", "status": "✅", "detail": "基金/指数产品不适用限售股解禁压力"})
    elif asset_type == "cn_stock":
        unlock_snapshot = _cn_stock_unlock_pressure_snapshot(metadata, context) or {
            "status": "ℹ️",
            "detail": "Tushare share_float 当前不可用，解禁压力暂未纳入本轮检查",
        }
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

        pledge_snapshot = _cn_pledge_risk_snapshot(metadata, context)
        checks.append(
            {
                "name": "质押风险",
                "status": str(pledge_snapshot.get("status", "ℹ️")),
                "detail": str(pledge_snapshot.get("detail", "Tushare pledge_stat / pledge_detail 当前不可用，质押风险暂未纳入本轮检查")),
            }
        )
        if pledge_snapshot.get("status") == "❌":
            exclusion_reasons.append("股权质押风险较高")
            warnings.append("⚠️ 股权质押比例偏高或股东质押集中度过大，遇到波动更容易放大筹码风险")
        elif pledge_snapshot.get("status") == "⚠️":
            warnings.append("⚠️ 当前存在一定股权质押压力，尤其在高波动或回撤阶段更需要保守看待")
    else:
        checks.append({"name": "解禁压力", "status": "ℹ️", "detail": "当前仅接入 A 股 Tushare 解禁日历"})
        if asset_type in {"hk", "us"}:
            checks.append({"name": "质押风险", "status": "ℹ️", "detail": "当前仅接入 A 股 Tushare 质押统计"})
        else:
            checks.append({"name": "质押风险", "status": "✅", "detail": "基金/指数产品不适用股权质押风险"})

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
            warnings.append(f"⚠️ 与 watchlist 中 `{peer}` 相关性过高，更适合作为同主题备选而不是组合新增仓位")
    else:
        checks.append({"name": "相关性", "status": "ℹ️", "detail": "相关性代理暂缺，未用于排除"})

    macro_ok = macro_score is None or macro_score > 0
    checks.append({"name": "宏观顺逆风", "status": "✅" if macro_ok else "⚠️", "detail": "按宏观敏感度维度做顺逆风修正"})
    if macro_score == 0:
        warnings.append("⚠️ 当前宏观敏感度完全逆风，评级和仓位都应更保守；这条约束已在评分层处理，不再额外做硬排除。")

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


def _support_reference_candidates(history: pd.DataFrame, technical: Mapping[str, Any]) -> List[tuple[float, str, float]]:
    close = history["close"].astype(float)
    low = history["low"].astype(float) if "low" in history.columns else close
    price = float(close.iloc[-1])
    ma_values = dict(technical.get("ma_system", {}).get("mas", {}) or {})
    fib_levels = dict(technical.get("fibonacci", {}).get("levels", {}) or {})
    candidates: List[tuple[float, str, float]] = []

    def _add(label: str, level: float) -> None:
        if level <= 0 or level >= price:
            return
        gap = float(price / level - 1.0)
        candidates.append((gap, label, float(level)))

    _add("MA20", float(ma_values.get("MA20", 0.0) or 0.0))
    _add("MA60", float(ma_values.get("MA60", 0.0) or 0.0))
    for level_name in ("0.382", "0.500", "0.618"):
        _add(f"斐波那契 {level_name}", float(fib_levels.get(level_name, 0.0) or 0.0))
    _add("近20日低点", float(low.tail(20).min()))
    _add("近60日低点", float(low.tail(60).min()))
    candidates.sort(key=lambda item: (item[0], item[2]))
    return candidates


def _nearest_support_reference(history: pd.DataFrame, technical: Mapping[str, Any]) -> tuple[str, float]:
    candidates = _support_reference_candidates(history, technical)
    if not candidates:
        return "", 0.0
    _, label, level = candidates[0]
    return label, level


def _pressure_signals(history: pd.DataFrame, technical: Mapping[str, Any]) -> tuple[int, str, float]:
    close = history["close"].astype(float)
    high = history["high"].astype(float)
    price = float(close.iloc[-1])
    ma20 = float(technical.get("ma_system", {}).get("mas", {}).get("MA20", 0.0))
    fib = technical.get("fibonacci", {})
    fib_levels = fib.get("levels", {})
    swing_high = float(fib.get("swing_high", 0.0))
    recent_high_20 = float(high.tail(20).max())
    recent_high_60 = float(high.tail(60).max())

    candidates: List[tuple[float, str, float]] = []

    def _add(label: str, level: float, threshold: float) -> None:
        if level <= 0 or level <= price:
            return
        gap = float(level / price - 1.0)
        if gap <= threshold:
            candidates.append((gap, label, float(level)))

    _add("MA20", ma20, 0.03)
    _add("斐波那契 0.786", float(fib_levels.get("0.786", 0.0)), 0.04)
    _add("近20日高点", recent_high_20, 0.05)
    _add("近60日高点", recent_high_60, 0.08)
    _add("摆动前高", swing_high, 0.08)

    deduped: List[tuple[float, str, float]] = []
    seen_labels: set[str] = set()
    for gap, label, level in sorted(candidates, key=lambda item: (item[0], item[2])):
        if label in seen_labels:
            continue
        deduped.append((gap, label, level))
        seen_labels.add(label)

    if deduped:
        nearest_pressure = float(deduped[0][2])
        detail = " / ".join(f"{label} {level:.3f}（上方 {gap:.1%}）" for gap, label, level in deduped[:3])
        if len(deduped) >= 2 and deduped[0][0] <= 0.03:
            score = -10
        elif deduped[0][0] <= 0.03:
            score = -8
        else:
            score = -5
        return score, f"上方存在近端压力：{detail}", nearest_pressure

    return 4, "上方最近明确压力不近，短线仍有继续试探空间", 0.0


def _technical_dimension(
    history: pd.DataFrame,
    technical: Mapping[str, Any],
    *,
    symbol: str = "",
    asset_type: str = "cn_stock",
    metadata: Optional[Mapping[str, Any]] = None,
    config: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    metadata = dict(metadata or {})
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

    divergence = technical.get("divergence", {})
    divergence_signal = str(divergence.get("signal", "neutral"))
    divergence_label = str(divergence.get("label", "未识别到明确顶/底背离"))
    divergence_detail = str(divergence.get("detail", "当前按最近两组确认摆点检查 RSI / MACD / OBV，未识别到明确背离。"))
    divergence_strength = int(divergence.get("strength", 0) or 0)
    divergence_age_days = _divergence_signal_age_days(divergence, history["date"].iloc[-1])
    if divergence_signal == "bullish":
        divergence_award = 10 if divergence_strength >= 2 else 6
    elif divergence_signal == "bearish":
        divergence_award = -8 if divergence_strength >= 2 else -4
    else:
        divergence_award = 0
    if divergence_signal != "neutral" and divergence_age_days is not None:
        if divergence_age_days > 7:
            divergence_award = 0
            divergence_detail += f" 当前最近一次确认点距今约 {divergence_age_days} 个交易日，已过背离触发窗口。"
        elif divergence_age_days > 5:
            divergence_award = int(divergence_award * 0.5)
            divergence_detail += f" 当前最近一次确认点距今约 {divergence_age_days} 个交易日，信号已进入衰减期。"
    raw += divergence_award
    available += 10
    factors.append(_factor_row("量价/动量背离", divergence_label, divergence_award, 10, f"背离按最近两组确认摆点识别，主要看价格与 RSI / MACD / OBV 是否同向。{divergence_detail}", factor_id="j1_divergence"))

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

    pressure_award, pressure_detail, _ = _pressure_signals(history, technical)
    raw += pressure_award
    available += 10
    factors.append(_factor_row("压力位", pressure_detail, pressure_award, 10, "优先看 MA20、斐波那契 0.786、近20/60日高点和摆动前高；上方压制越近，反弹越容易先进入承压消化。", factor_id="j1_resistance_zone"))

    pattern_labels = {
        "morning_star": "早晨之星",
        "evening_star": "黄昏之星",
        "three_white_soldiers": "红三兵",
        "three_black_crows": "三只乌鸦",
        "three_inside_up": "三内升",
        "three_inside_down": "三内降",
        "bullish_engulfing": "看涨吞没",
        "bearish_engulfing": "看跌吞没",
        "bullish_harami": "看涨母子线",
        "bearish_harami": "看跌母子线",
        "piercing_line": "曙光初现",
        "dark_cloud_cover": "乌云盖顶",
        "tweezer_bottom": "平底镊子线",
        "tweezer_top": "平顶镊子线",
        "hammer": "锤头线",
        "inverted_hammer": "倒锤头",
        "shooting_star": "流星线",
        "hanging_man": "上吊线",
        "bullish_marubozu": "光头光脚长阳",
        "bearish_marubozu": "光头光脚长阴",
        "marubozu": "长实体 K 线",
        "doji": "十字星",
    }
    pattern_scores = {
        "morning_star": 10,
        "three_white_soldiers": 10,
        "three_inside_up": 8,
        "bullish_engulfing": 8,
        "bullish_harami": 5,
        "piercing_line": 7,
        "tweezer_bottom": 5,
        "hammer": 6,
        "inverted_hammer": 5,
        "bullish_marubozu": 6,
        "evening_star": -10,
        "three_black_crows": -10,
        "three_inside_down": -8,
        "bearish_engulfing": -8,
        "bearish_harami": -5,
        "dark_cloud_cover": -7,
        "tweezer_top": -5,
        "shooting_star": -6,
        "hanging_man": -6,
        "bearish_marubozu": -6,
        "doji": 0,
        "marubozu": 0,
    }
    patterns = list(technical.get("candlestick", []) or [])
    if "bullish_marubozu" in patterns or "bearish_marubozu" in patterns:
        patterns = [item for item in patterns if item != "marubozu"]
    recognized_patterns = [item for item in patterns if item in pattern_scores]
    bullish_scores = [pattern_scores[item] for item in recognized_patterns if pattern_scores[item] > 0]
    bearish_scores = [pattern_scores[item] for item in recognized_patterns if pattern_scores[item] < 0]
    strongest_bullish = max(bullish_scores, default=0)
    strongest_bearish = min(bearish_scores, default=0)
    if abs(strongest_bearish) > abs(strongest_bullish):
        candle_award = strongest_bearish
    elif strongest_bullish > abs(strongest_bearish):
        candle_award = strongest_bullish
    else:
        candle_award = strongest_bullish if strongest_bullish and not bearish_scores else 0
    raw += candle_award
    available += 10
    pattern_signal = " / ".join(pattern_labels.get(item, item) for item in recognized_patterns[:3]) if recognized_patterns else "无明确组合形态"
    factors.append(
        _factor_row(
            "K线形态",
            pattern_signal,
            candle_award,
            10,
            "当前按最近 1-3 根 K 线识别单根、双根、三根组合形态；吞没/星形/三兵三鸦等反转形态会结合前序 5 日趋势过滤。",
            factor_id="j1_candlestick",
        )
    )

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
    factors.append(_factor_row("量价结构", f"{structure} · 量能比5日 {vol_ratio:.2f} / 20日 {vol_ratio_20:.2f}", volume_award, 15, f"这里先看日度量价结构：放量突破更像趋势确认，缩量回调更像抛压衰减，放量滞涨/放量下跌更像分歧扩大；当日涨跌幅 {latest_return:.1%}{amount_text}", factor_id="j1_volume_structure"))

    stock_factor_snapshot: Dict[str, Any] = {}
    if asset_type == "cn_stock" and str(symbol).strip():
        as_of = ""
        if not history.empty and "date" in history.columns:
            as_of = pd.to_datetime(history["date"].iloc[-1], errors="coerce")
            if pd.notna(as_of):
                as_of = as_of.strftime("%Y-%m-%d")
            else:
                as_of = ""
        try:
            stock_factor_snapshot = ValuationCollector(dict(config or {})).get_cn_stock_factor_snapshot(
                symbol,
                as_of=as_of,
            )
        except Exception:
            stock_factor_snapshot = {}

    stock_factor_status = str(stock_factor_snapshot.get("status", "")).strip()
    stock_factor_fresh = bool(stock_factor_snapshot.get("is_fresh"))
    stock_factor_trend = str(stock_factor_snapshot.get("trend_label", "")).strip()
    stock_factor_momentum = str(stock_factor_snapshot.get("momentum_label", "")).strip()
    stock_factor_detail = str(stock_factor_snapshot.get("detail", "")).strip()
    stock_factor_date = str(stock_factor_snapshot.get("latest_date", "")).strip()
    if stock_factor_status == "matched" and stock_factor_trend:
        if stock_factor_trend == "趋势偏强" and stock_factor_momentum == "动能改善":
            stock_factor_award = 12
        elif stock_factor_trend in {"趋势偏强", "修复中"} and stock_factor_momentum != "动能偏弱":
            stock_factor_award = 8
        elif stock_factor_trend == "趋势偏弱" and stock_factor_momentum == "动能偏弱":
            stock_factor_award = -8
        elif stock_factor_trend == "趋势偏弱":
            stock_factor_award = -5
        elif stock_factor_momentum == "动能改善":
            stock_factor_award = 5
        elif stock_factor_momentum == "动能偏弱":
            stock_factor_award = -4
        else:
            stock_factor_award = 0
        raw += stock_factor_award
        available += 12
        factor_detail = "Tushare stk_factor_pro 股票每日技术面因子；先把收盘技术状态当成独立证据，不再只靠价格均线肉眼猜。"
        if stock_factor_detail:
            factor_detail = f"{factor_detail} {stock_factor_detail}"
        factor_signal = stock_factor_trend
        if stock_factor_momentum:
            factor_signal += f" / {stock_factor_momentum}"
        if stock_factor_date:
            factor_signal += f"（{stock_factor_date}）"
        factors.append(
            _factor_row(
                "股票技术面状态",
                factor_signal,
                stock_factor_award,
                12,
                factor_detail,
                factor_id="j1_stk_factor_pro",
                factor_meta_overrides={
                    "source_as_of": stock_factor_date or None,
                    "degraded": not stock_factor_fresh,
                    "degraded_reason": (
                        "Tushare stk_factor_pro 最新日期落后于当前可用交易日，不按 fresh 命中。"
                        if stock_factor_status == "matched" and not stock_factor_fresh
                        else None
                    ),
                },
            )
        )
    elif asset_type == "cn_stock":
        if stock_factor_status == "empty":
            factors.append(
                _factor_row(
                    "股票技术面状态",
                    "stk_factor_pro 数据缺失",
                    None,
                    12,
                    "Tushare stk_factor_pro 当前未返回可用股票技术因子；不把空结果写成今天趋势已确认。",
                    display_score="信息项",
                    factor_id="j1_stk_factor_pro",
                    factor_meta_overrides={"degraded": True, "degraded_reason": "Tushare stk_factor_pro empty"},
                )
            )
            available += 12
        elif stock_factor_status == "blocked":
            factors.append(
                _factor_row(
                    "股票技术面状态",
                    "stk_factor_pro 缺失",
                    None,
                    12,
                    str(stock_factor_snapshot.get("disclosure", "")).strip() or "Tushare stk_factor_pro 当前不可用；本轮按缺失处理。",
                    display_score="信息项",
                    factor_id="j1_stk_factor_pro",
                    factor_meta_overrides={"degraded": True, "degraded_reason": str(stock_factor_snapshot.get("diagnosis", "blocked"))},
                )
            )

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
    factors.append(_factor_row("波动压缩", f"ATR/收盘 {natr:.2%} · 带宽分位 {width_pct:.0%}", vol_award, 10, "波动压缩更像筹码收敛后的启动前状态；如果 ATR 和布林带宽度同步扩张，通常意味着已经进入情绪释放阶段而不是舒服的低吸区", factor_id="j1_volatility_compression"))

    setup = technical.get("setup", {})
    false_break = dict(setup.get("false_break") or {})
    support_setup_block = dict(setup.get("support_setup") or {})
    compression_setup_block = dict(setup.get("compression_setup") or {})
    false_break_kind = str(false_break.get("kind", "none"))
    false_break_label = str(false_break.get("label", "未识别到明确假突破形态"))
    support_kind = str(support_setup_block.get("kind", "support_intact"))
    support_label = str(support_setup_block.get("label", "支撑位完整"))
    compression_kind = str(compression_setup_block.get("kind", "neutral"))
    compression_label = str(compression_setup_block.get("label", "量价压缩状态中性"))

    if false_break_kind == "bearish_false_break":
        false_break_award = 8
    elif false_break_kind == "bullish_false_break":
        false_break_award = -8
    else:
        false_break_award = 0
    raw += false_break_award
    available += 8
    factors.append(_factor_row("假突破识别", false_break_label, false_break_award, 8, "假突破是多空双方试探失败的信号：看涨假突破说明多头未能守住突破位，看跌假突破说明空头未能守住跌破位，两者都是方向反转的早期线索", factor_id="j1_false_break"))

    if support_kind == "support_intact":
        support_award = 5
    elif support_kind in {"failed_recovery", "breakdown_continuation"}:
        support_award = -8
    elif support_kind == "breakdown_watching":
        support_award = -3
    else:
        support_award = 0
    raw += support_award
    available += 8
    factors.append(_factor_row("支撑结构", support_label, support_award, 8, "支撑失效后的分流很重要：反弹未收复支撑位是失效确认，继续下行是趋势延续，两者都偏空；支撑完整则是多头的基础条件", factor_id="j1_support_setup"))

    if compression_kind == "compression_breakout":
        compression_award = 10
    elif compression_kind == "momentum_chase":
        compression_award = -5
    elif compression_kind == "still_compressing":
        compression_award = 3
    else:
        compression_award = 0
    raw += compression_award
    available += 10
    factors.append(_factor_row("压缩启动", compression_label, compression_award, 10, "压缩后放量启动是最干净的介入 setup：波动收敛说明筹码趋于稳定，放量突破说明有新资金介入；情绪追价区则相反，波动已扩张时追涨更像接最后一棒", factor_id="j1_compression_breakout"))

    score = _normalize_dimension(raw, available, 100)
    stock_factor_note = ""
    if stock_factor_status == "matched":
        stock_factor_note = (
            f" 当前已接入 Tushare stk_factor_pro：{stock_factor_trend}"
            + (f" / {stock_factor_momentum}" if stock_factor_momentum else "")
            + ("；但最新日期仍有缺口" if not stock_factor_fresh else "")
        )
    return {
        "name": "技术面",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(score, "价格结构到位，技术信号共振较强。", "技术面有亮点，但还没有形成满配共振。", "技术结构仍偏弱，暂不支持激进介入。", "ℹ️ 技术面数据缺失，本次评级未纳入该维度") + stock_factor_note,
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
        "stock_factor_snapshot": stock_factor_snapshot,
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


def _j5_etf_fund_factors(
    asset_type: str,
    metadata: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]],
) -> tuple[List[Dict[str, Any]], int, int]:
    """J-5: ETF / 基金专属因子评分模块.

    Covers: ETF折溢价、ETF份额申赎、跟踪误差、成分集中度、主题纯度、
            业绩基准披露（场外基金）、风格漂移评估、经理稳定性、费率结构.

    Returns (factors, raw_added, available_added).
    All factors carry explicit proxy_level disclosure per J-5 hard constraints.
    Factor IDs are wired to FACTOR_REGISTRY entries for downstream consumers
    (e.g. decision_review, strategy) to filter by state/visibility_class.
    """
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    if asset_type not in {"cn_etf", "cn_fund"}:
        return factors, raw, available

    style = dict((fund_profile or {}).get("style") or {})
    overview = dict((fund_profile or {}).get("overview") or {})
    manager_info = dict((fund_profile or {}).get("manager") or {})
    rating = dict((fund_profile or {}).get("rating") or {})
    fund_factor_snapshot = dict((fund_profile or {}).get("fund_factor_snapshot") or {})

    tags = list(style.get("tags") or [])
    is_passive = "被动跟踪" in tags or bool(metadata.get("is_passive_fund"))
    is_etf = asset_type == "cn_etf"
    is_active_fund = asset_type == "cn_fund" and not is_passive
    sector = str(style.get("sector") or metadata.get("sector") or "").strip()
    benchmark_note = str(style.get("benchmark_note") or overview.get("业绩比较基准", "")).strip()

    # J5.1: ETF 折溢价 (ETF 专属, daily_close, direct)
    # Hard constraint: only applies to exchange-listed ETFs with intraday pricing.
    if is_etf:
        premium_rate = None
        for key in ("premium_rate", "discount_rate"):
            raw_val = metadata.get(key)
            if raw_val is not None:
                try:
                    premium_rate = float(raw_val)
                    break
                except (ValueError, TypeError):
                    pass
        if premium_rate is not None:
            abs_p = abs(premium_rate)
            if abs_p > 3:
                p_award = 0
                p_signal = f"折溢价偏极端 {premium_rate:+.2f}%，需关注流动性风险"
            elif premium_rate < -1:
                p_award = 10
                p_signal = f"折价 {premium_rate:.2f}%，场内价格低于 NAV，存在修复空间"
            elif premium_rate > 1:
                p_award = 0
                p_signal = f"溢价 {premium_rate:.2f}%，场内价格高于 NAV，回落风险存在"
            else:
                p_award = 5
                p_signal = f"折溢价 {premium_rate:+.2f}%（接近 NAV 中性区间）"
            raw += p_award
            available += 10
            factors.append(_factor_row(
                "ETF 折溢价", p_signal, p_award, 10,
                "ETF 场内价格相对 NAV 的偏离；折价意味着场内买入比按 NAV 申购更划算，溢价时反之。数据源：实时行情（direct，无 lag）。",
                factor_id="j5_etf_premium",
            ))
        else:
            factors.append(_factor_row(
                "ETF 折溢价", "实时折溢价数据缺失", None, 10,
                "ETF 折溢价需实时行情对比 NAV；当前未接入。",
                display_score="信息项",
                factor_id="j5_etf_premium",
            ))

    # J5.1b: ETF 份额申赎 (ETF 专属, T+1, direct — j5_etf_share_change)
    # 份额净创设 = 机构正在场外申购（偏多），净赎回 = 机构在赎回（偏空）。
    if is_etf:
        share_change = None
        for key in ("etf_share_change", "share_change"):
            raw_val = metadata.get(key)
            if raw_val is not None:
                try:
                    share_change = float(raw_val)
                    break
                except (ValueError, TypeError):
                    pass
        if share_change is not None:
            if share_change > 5:
                sc_award = 10
                sc_signal = f"ETF 份额净创设 +{share_change:.1f}（亿份），机构积极申购，资金净流入"
            elif share_change > 0:
                sc_award = 7
                sc_signal = f"ETF 份额小幅净创设 +{share_change:.1f}（亿份），资金温和流入"
            elif share_change >= -2:
                sc_award = 4
                sc_signal = f"ETF 份额基本持平 {share_change:+.1f}（亿份），申赎中性"
            else:
                sc_award = 0
                sc_signal = f"ETF 份额净赎回 {share_change:.1f}（亿份），存在赎回压力"
            raw += sc_award
            available += 10
            factors.append(_factor_row(
                "ETF 份额申赎", sc_signal, sc_award, 10,
                "ETF 份额变化反映场外申购/赎回行为；净创设代表机构资金流入（T+1 可见，direct）。数据源：基金份额日报（T+1 lag）。",
                factor_id="j5_etf_share_change",
            ))
        else:
            factors.append(_factor_row(
                "ETF 份额申赎", "份额申赎数据缺失", None, 10,
                "ETF 份额变化（净创设/净赎回）数据未接入，无法评估资金流向；数据源：基金份额日报（T+1 lag）。",
                display_score="信息项",
                factor_id="j5_etf_share_change",
            ))

    fund_factor_trend = str(fund_factor_snapshot.get("trend_label", "")).strip()
    fund_factor_momentum = str(fund_factor_snapshot.get("momentum_label", "")).strip()
    fund_factor_detail = str(fund_factor_snapshot.get("detail", "")).strip()
    fund_factor_date = str(fund_factor_snapshot.get("latest_date", "") or fund_factor_snapshot.get("trade_date", "")).strip()
    if fund_factor_trend:
        if fund_factor_trend == "趋势偏强" and fund_factor_momentum == "动能改善":
            ff_award = 10
        elif fund_factor_trend in {"趋势偏强", "修复中"} and fund_factor_momentum != "动能偏弱":
            ff_award = 8
        elif fund_factor_trend == "震荡":
            ff_award = 5
        else:
            ff_award = 2
        ff_signal = f"场内基金技术因子 {fund_factor_trend}"
        if fund_factor_momentum:
            ff_signal += f" / {fund_factor_momentum}"
        if fund_factor_date:
            ff_signal += f"（{fund_factor_date}）"
        ff_detail = "场内基金技术状态来自 Tushare fund_factor_pro（收盘后技术因子）；优先用来确认产品层趋势与动能。"
        if fund_factor_detail:
            ff_detail += f" {fund_factor_detail}"
        raw += ff_award
        available += 10
        factors.append(
            _factor_row(
                "场内基金技术状态",
                ff_signal,
                ff_award,
                10,
                ff_detail,
                factor_id="j5_fund_factor_pro",
            )
        )
    elif is_etf:
        factors.append(
            _factor_row(
                "场内基金技术状态",
                "fund_factor_pro 数据缺失",
                None,
                10,
                "场内基金技术面因子当前缺失；不把产品层技术状态伪装成已确认。数据源：Tushare fund_factor_pro。",
                display_score="信息项",
                factor_id="j5_fund_factor_pro",
            )
        )

    # J5.2: 跟踪误差 (ETF + 被动基金, daily_close, direct — j5_tracking_error)
    # 优先使用实际年化跟踪误差数值；数据缺失时降级为基准清晰度代理（proxy 分扣减）。
    # Hard constraint: only for exchange-listed ETFs and passive funds.
    if is_etf or is_passive:
        tracking_error = None
        for key in ("tracking_error", "annualized_tracking_error"):
            raw_val = metadata.get(key)
            if raw_val is not None:
                try:
                    tracking_error = float(raw_val)
                    break
                except (ValueError, TypeError):
                    pass
        if tracking_error is not None:
            if tracking_error < 0.3:
                track_award = 10
                track_signal = f"年化跟踪误差 {tracking_error:.2f}%（优秀，偏离极小）"
            elif tracking_error < 0.5:
                track_award = 8
                track_signal = f"年化跟踪误差 {tracking_error:.2f}%（良好）"
            elif tracking_error < 1.0:
                track_award = 5
                track_signal = f"年化跟踪误差 {tracking_error:.2f}%（偏高，需关注）"
            else:
                track_award = 2
                track_signal = f"年化跟踪误差 {tracking_error:.2f}%（较高，跟踪质量存疑）"
            track_detail = f"年化跟踪误差 {tracking_error:.2f}%（直接数据，daily_close，无 lag）；越低越好。"
        else:
            # 数据缺失，降级为基准清晰度代理
            if benchmark_note and benchmark_note != "未披露业绩比较基准":
                consistency_text = str(style.get("consistency") or "").lower()
                if "跟踪" in consistency_text and ("误差" in consistency_text or "偏离" in consistency_text):
                    track_award = 4
                    track_signal = f"跟踪基准已披露（{benchmark_note[:35]}），但存在跟踪提示（代理）"
                else:
                    track_award = 6
                    track_signal = f"跟踪基准清晰（{benchmark_note[:35]}）；实际跟踪误差数据未接入"
                track_detail = "实际跟踪误差数据未接入，以基准清晰度代理评分（已下调，非直接数据）。数据源：基金合同（季度更新）。"
            else:
                track_award = 0
                track_signal = "业绩基准未披露，无法评估跟踪误差"
                track_detail = "业绩基准不清晰是跟踪风险的前提条件；无法评估跟踪误差。数据源：基金合同。"
        raw += track_award
        available += 10
        factors.append(_factor_row(
            "跟踪误差", track_signal, track_award, 10, track_detail,
            factor_id="j5_tracking_error",
        ))

    # J5.3: 成分股集中度 (ETF + 基金, quarterly, direct — 季报 lag 30~45 天)
    top5 = float(style.get("top5_concentration") or 0.0)
    if top5 > 0:
        if top5 >= 70:
            conc_award = 5
            conc_signal = f"前五大重仓合计 {top5:.1f}%（高度集中，集中度风险明显）"
        elif top5 >= 30:
            conc_award = 10
            conc_signal = f"前五大重仓合计 {top5:.1f}%（集中度适中）"
        else:
            conc_award = 8
            conc_signal = f"前五大重仓合计 {top5:.1f}%（高度分散）"
        raw += conc_award
        available += 10
        factors.append(_factor_row(
            "成分集中度", conc_signal, conc_award, 10,
            "前五大重仓占净值比例代理集中度；集中度高则暴露更纯粹，但个股风险更高。数据源：最近一期季报（T+30~45 天 lag）。",
            factor_id="j5_component_concentration",
        ))
    else:
        factors.append(_factor_row(
            "成分集中度", "持仓明细缺失", None, 10,
            "当前未拿到前五大持仓，无法评估成分集中度；数据源：季报（T+30~45 天 lag）。",
            factor_id="j5_component_concentration",
        ))

    # J5.4: 主题纯度 (ETF + 基金, quarterly, direct)
    if sector and sector != "综合":
        if is_passive:
            bench_lower = benchmark_note.lower()
            sector_match = sector.lower() in bench_lower
            purity_award = 10 if sector_match else 7
            purity_signal = f"主题 `{sector}`，基准{'与主题匹配' if sector_match else '匹配度有限'}"
        else:
            purity_award = 8
            purity_signal = f"主题 `{sector}` 已从持仓识别（主动基金代理，持仓季报 T+45 天）"
    else:
        purity_award = 0
        purity_signal = "主题/行业方向不清晰（综合型或无法从持仓识别）"
    raw += purity_award
    available += 10
    factors.append(_factor_row(
        "主题纯度", purity_signal, purity_award, 10,
        "主题越纯，行业 beta 越清晰，轮动逻辑越充分。数据源：基金名称/基准/持仓（季报 lag）。",
        factor_id="j5_theme_purity",
    ))

    # J5.5: 业绩基准披露（场外基金专属, quarterly, direct）
    if asset_type == "cn_fund":
        bm_raw = str(overview.get("业绩比较基准", "")).strip()
        if bm_raw and bm_raw != "未披露业绩比较基准" and len(bm_raw) > 5:
            bm_award = 10
            bm_signal = f"业绩基准已披露：{bm_raw[:40]}"
        elif bm_raw:
            bm_award = 5
            bm_signal = f"业绩基准简短披露：{bm_raw}"
        else:
            bm_award = 0
            bm_signal = "业绩比较基准未披露"
        raw += bm_award
        available += 10
        factors.append(_factor_row(
            "业绩基准披露", bm_signal, bm_award, 10,
            "场外基金业绩基准是判断超额来源和风格漂移的基础；基准不清晰则收益归因无从开始。数据源：基金合同/定期报告（direct）。",
            factor_id="j5_fund_benchmark_fit",
        ))

    # J5.6: 风格漂移评估（主动基金专属, quarterly proxy）
    if is_active_fund:
        has_stable_tag = "风格稳定" in tags
        consistency_raw = str(style.get("consistency") or "")
        if has_stable_tag:
            drift_award = 10
            drift_signal = "风格一致性强（在管产品暴露方向一致）"
        elif len(consistency_raw) > 20:
            drift_award = 6
            drift_signal = "风格一致性有一定支撑，但样本有限"
        else:
            drift_award = 0
            drift_signal = "风格一致性数据不足，无法评估漂移风险"
        raw += drift_award
        available += 10
        factors.append(_factor_row(
            "风格漂移评估", drift_signal, drift_award, 10,
            "主动基金风格漂移是核心风险：持仓和基准偏离越大，暴露越难预测。当前以经理在管产品命名/持仓方向代理，数据源：季报持仓（T+45 天 lag）。",
            factor_id="j5_style_drift",
        ))

    # J5.7: 基金经理稳定性（all funds, event_driven, direct）
    tenure_days_val = manager_info.get("tenure_days")
    if tenure_days_val is not None:
        try:
            tenure_years = float(tenure_days_val) / 365.25
            if tenure_years >= 5:
                mgr_award = 10
                mgr_signal = f"在职 {tenure_years:.1f} 年（历经至少一个完整牛熊周期）"
            elif tenure_years >= 3:
                mgr_award = 7
                mgr_signal = f"在职 {tenure_years:.1f} 年（中等经验）"
            elif tenure_years >= 1:
                mgr_award = 4
                mgr_signal = f"在职 {tenure_years:.1f} 年（新晋经理，历史较短）"
            else:
                mgr_award = 1
                mgr_signal = f"在职 {tenure_years:.1f} 年（非常新，风格尚未验证）"
            raw += mgr_award
            available += 10
            factors.append(_factor_row(
                "经理稳定性", mgr_signal, mgr_award, 10,
                "基金经理任职年限是衡量经验深度和团队稳定性的基础；新经理历史短，风格尚未经过牛熊验证。数据源：基金管理人档案（事件驱动更新）。",
                factor_id="j5_manager_stability",
            ))
        except (ValueError, TypeError):
            factors.append(_factor_row("经理稳定性", "任职时长数据异常", None, 10, "基金经理稳定性评估数据解析失败。",
                                       factor_id="j5_manager_stability"))
    else:
        factors.append(_factor_row(
            "经理稳定性", "经理任职数据缺失", None, 10,
            "基金经理稳定性评估需任职年限；当前数据未接入。",
            factor_id="j5_manager_stability",
        ))

    # J5.8: 申赎友好度 / 费率结构 (all, quarterly proxy)
    fee_str = str(overview.get("管理费率") or "").strip()
    fee_value: Optional[float] = None
    if fee_str:
        fm = re.search(r"(\d+(?:\.\d+)?)", fee_str)
        if fm:
            try:
                fee_value = float(fm.group(1))
            except ValueError:
                pass
    if fee_value is None:
        rating_fee = rating.get("fee")
        if rating_fee is not None:
            try:
                fee_value = float(rating_fee)
            except (ValueError, TypeError):
                pass
    if fee_value is not None:
        if fee_value <= 0.5:
            fee_award = 10
            fee_signal = f"管理费率 {fee_value:.2f}% / 年（被动/低费）"
        elif fee_value <= 1.0:
            fee_award = 7
            fee_signal = f"管理费率 {fee_value:.2f}% / 年（费率适中）"
        elif fee_value <= 1.5:
            fee_award = 4
            fee_signal = f"管理费率 {fee_value:.2f}% / 年（费率偏高）"
        else:
            fee_award = 0
            fee_signal = f"管理费率 {fee_value:.2f}% / 年（高费率，长期持有磨损大）"
        raw += fee_award
        available += 10
        factors.append(_factor_row(
            "费率结构", fee_signal, fee_award, 10,
            "管理费率越低，长期持有成本越小；ETF/指数型通常费率极低（0.1-0.5%），主动基金通常 1.0-1.5%。数据源：基金合同（quarterly 更新）。",
            factor_id="j5_redemption_pressure",
        ))
    else:
        factors.append(_factor_row(
            "费率结构", "费率数据缺失", None, 10,
            "管理费率数据缺失，申赎友好度无法量化评估。",
            factor_id="j5_redemption_pressure",
        ))

    return factors, raw, available


def _fundamental_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    metrics: Mapping[str, float],
    config: Mapping[str, Any],
    fund_profile: Optional[Mapping[str, Any]] = None,
    context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    display_name = "产品质量/基本面代理" if asset_type in {"cn_etf", "cn_index", "cn_fund"} else "基本面"
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    valuation_snapshot: Optional[Dict[str, Any]] = None
    valuation_note = f"近一年价格分位 {price_percentile:.0%}，这只反映位置，不等于真实估值分位。"
    valuation_history = pd.DataFrame()
    financial_proxy: Dict[str, Any] = {}
    sector_flow = {}
    commodity_like_fund = _is_commodity_like_fund(asset_type, metadata, fund_profile)
    index_topic_bundle = dict(metadata.get("index_topic_bundle") or {})
    prefetched_index_snapshot = dict(index_topic_bundle.get("index_snapshot") or {})
    fund_profile_mode = str((fund_profile or {}).get("profile_mode") or "").strip().lower()
    is_light_etf_profile = asset_type == "cn_etf" and fund_profile_mode == "light"

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

        commodity_text = " ".join(
            str(part).strip()
            for part in (
                metadata.get("name", ""),
                metadata.get("sector", ""),
                benchmark,
                fund_type,
            )
            if str(part).strip()
        ).lower()
        if any(token in commodity_text for token in ("黄金", "贵金属", "au99", "上海金")):
            try:
                gold_frame = CommodityCollector(config).get_gold()
            except Exception:
                gold_frame = pd.DataFrame()
            gold_date = str(getattr(gold_frame, "attrs", {}).get("latest_date", "") or "").strip()
            gold_fresh = bool(getattr(gold_frame, "attrs", {}).get("is_fresh", False))
            gold_source = str(getattr(gold_frame, "attrs", {}).get("source", "tushare.sge_daily")).strip() or "tushare.sge_daily"
            gold_disclosure = str(getattr(gold_frame, "attrs", {}).get("disclosure", "")).strip() or "黄金现货日线当前缺失。"
            latest_close = None
            previous_close = None
            if isinstance(gold_frame, pd.DataFrame) and not gold_frame.empty and "close" in gold_frame.columns:
                close_series = pd.to_numeric(gold_frame["close"], errors="coerce").dropna()
                if not close_series.empty:
                    latest_close = float(close_series.iloc[-1])
                    if len(close_series) >= 2:
                        previous_close = float(close_series.iloc[-2])
            pct_text = ""
            if latest_close is not None and previous_close not in (None, 0):
                pct_text = f"，日变动 {((latest_close / previous_close) - 1.0):+.2%}"
            if latest_close is not None and gold_fresh:
                gold_award = 10
                raw += gold_award
                available += 10
                signal = f"上海金 Au99.95 最新收于 {latest_close:.2f}（{gold_date or '最新'}）{pct_text}"
                detail = f"{gold_source} 已接入黄金现货锚定；这里只用来确认产品贴近黄金现货链条，不把单日涨跌直接写成买卖结论。"
                factors.append(_factor_row("现货锚定", signal, gold_award, 10, detail, factor_id="j5_gold_spot_anchor"))
            else:
                signal = "黄金现货锚定暂缺或非当期"
                detail = gold_disclosure
                factors.append(_factor_row("现货锚定", signal, None, 10, detail, display_score="观察", factor_id="j5_gold_spot_anchor"))

        if asset_type == "cn_fund":
            sales_ratio_snapshot = dict((fund_profile or {}).get("sales_ratio_snapshot") or {})
            if sales_ratio_snapshot:
                latest_year = str(sales_ratio_snapshot.get("latest_year", "") or "").strip()
                lead_channel = str(sales_ratio_snapshot.get("lead_channel", "") or "").strip()
                lead_ratio = sales_ratio_snapshot.get("lead_ratio")
                signal = sales_ratio_snapshot.get("summary") or "公募渠道保有结构已接入。"
                detail = (
                    f"{latest_year or '最新'} 年销售保有结构里，`{lead_channel or '主导渠道'}` 占比"
                    f"{f'约 {float(lead_ratio):.2f}%' if lead_ratio not in (None, '') else '居前'}。"
                    " 这是行业级渠道环境，不直接等于单只基金申购强弱。"
                )
                factors.append(
                    _factor_row(
                        "公募渠道环境",
                        str(signal),
                        0,
                        0,
                        detail,
                        display_score="信息项",
                        factor_id="j5_fund_sales_ratio",
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
            "display_name": display_name,
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
        valuation_snapshot = dict(prefetched_index_snapshot) if prefetched_index_snapshot else None
        if not valuation_snapshot:
            try:
                valuation_snapshot = collector.get_cn_index_snapshot(_valuation_keywords(metadata, asset_type, fund_profile))
            except Exception:
                valuation_snapshot = None
        if valuation_snapshot and not is_light_etf_profile:
            try:
                valuation_history = collector.get_cn_index_value_history(str(valuation_snapshot.get("index_code", "")))
            except Exception:
                valuation_history = pd.DataFrame()
        if not is_light_etf_profile:
            try:
                if asset_type in {"cn_fund", "cn_etf"} and fund_profile:
                    financial_proxy = _fund_financial_proxy(collector, fund_profile)
                elif valuation_snapshot:
                    financial_proxy = collector.get_cn_index_financial_proxies(str(valuation_snapshot.get("index_code", "")), top_n=5)
            except Exception:
                financial_proxy = {}
        if asset_type in {"cn_fund", "cn_etf"} and fund_profile and valuation_snapshot:
            match_quality = str(valuation_snapshot.get("match_quality", "")).strip()
            if match_quality in {"exact_no_pe", "benchmark_no_proxy"}:
                holdings_proxy = _fund_holdings_valuation_proxy(collector, fund_profile, top_n=5)
                if holdings_proxy:
                    for key, value in holdings_proxy.items():
                        if financial_proxy.get(key) is None:
                            financial_proxy[key] = value
                holdings_pe = holdings_proxy.get("pe_ttm")
                coverage_weight = float(holdings_proxy.get("coverage_weight", 0.0) or 0.0)
                coverage_ratio = float(holdings_proxy.get("coverage_ratio", 0.0) or 0.0)
                if holdings_pe is not None and (coverage_ratio >= 0.35 or coverage_weight >= 35.0):
                    benchmark_name = str(valuation_snapshot.get("index_name", "相关指数")).strip() or "相关指数"
                    valuation_snapshot = {
                        **valuation_snapshot,
                        "pe_ttm": float(holdings_pe),
                        "metric_label": "前五大重仓加权PE",
                        "display_label": "真实基准重仓股PE代理",
                        "match_quality": "exact_holdings_proxy" if match_quality == "exact_no_pe" else "benchmark_holdings_proxy",
                        "match_note": (
                            (
                                f"估值库已命中 `{benchmark_name}`，但缺少直接滚动PE；"
                                if match_quality == "exact_no_pe"
                                else f"估值库未直接命中 `{benchmark_name}`，"
                            )
                            + f"当前改用最近一期前五大重仓加权PE代理（覆盖约 {coverage_weight:.1f}% 持仓）。"
                        ),
                    }
        try:
            sector_flow = _sector_flow_snapshot(metadata, _context_drivers(context or {}, config))
        except Exception:
            sector_flow = {}

    sales_ratio_snapshot = dict((fund_profile or {}).get("sales_ratio_snapshot") or {})
    if asset_type == "cn_fund" and sales_ratio_snapshot:
        latest_year = str(sales_ratio_snapshot.get("latest_year", "") or "").strip()
        lead_channel = str(sales_ratio_snapshot.get("lead_channel", "") or "").strip()
        lead_ratio = sales_ratio_snapshot.get("lead_ratio")
        signal = sales_ratio_snapshot.get("summary") or "公募渠道保有结构已接入。"
        detail = (
            f"{latest_year or '最新'} 年销售保有结构里，`{lead_channel or '主导渠道'}` 占比"
            f"{f'约 {float(lead_ratio):.2f}%' if lead_ratio not in (None, '') else '居前'}。"
            " 这是行业级渠道环境，不直接等于单只基金申购强弱。"
        )
        factors.append(
            _factor_row(
                "公募渠道环境",
                str(signal),
                0,
                0,
                detail,
                display_score="信息项",
                factor_id="j5_fund_sales_ratio",
            )
        )

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
            sector_flow = _sector_flow_snapshot(metadata, _context_drivers(context or {}, config))
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

    if asset_type == "cn_stock":
        convertible_bond_snapshot = _cn_stock_convertible_bond_snapshot(metadata, context or {})
        cb_status = str(convertible_bond_snapshot.get("status", "")).strip()
        cb_is_fresh = bool(convertible_bond_snapshot.get("is_fresh"))
        cb_trend = str(convertible_bond_snapshot.get("trend_label", "")).strip()
        cb_momentum = str(convertible_bond_snapshot.get("momentum_label", "")).strip()
        cb_detail = str(convertible_bond_snapshot.get("detail", "")).strip()
        cb_latest_date = str(convertible_bond_snapshot.get("latest_date", "")).strip()
        cb_row = dict(convertible_bond_snapshot.get("row") or {})
        cb_premium_rate = pd.to_numeric(pd.Series([convertible_bond_snapshot.get("premium_rate")]), errors="coerce").iloc[0]
        cb_remain_size_yi = pd.to_numeric(pd.Series([convertible_bond_snapshot.get("remain_size_yi")]), errors="coerce").iloc[0]
        if cb_status == "matched":
            if cb_is_fresh:
                cb_award = 0
                if cb_premium_rate == cb_premium_rate:
                    if float(cb_premium_rate) <= 0:
                        cb_award = 8
                    elif float(cb_premium_rate) <= 20:
                        cb_award = 5
                    elif float(cb_premium_rate) <= 50:
                        cb_award = 2
                    else:
                        cb_award = -3
                if cb_trend == "趋势偏强":
                    cb_award = min(cb_award + 2, 10)
                elif cb_trend == "趋势偏弱":
                    cb_award = max(cb_award - 2, -6)
                if cb_momentum == "动能改善":
                    cb_award = min(cb_award + 1, 10)
                elif cb_momentum == "动能偏弱":
                    cb_award = max(cb_award - 1, -6)
                raw += cb_award
                available += 10
                cb_signal_parts = [str(cb_row.get("bond_short_name") or "可转债映射")]
                if cb_trend:
                    cb_signal_parts.append(cb_trend)
                if cb_momentum:
                    cb_signal_parts.append(cb_momentum)
                if cb_premium_rate == cb_premium_rate:
                    cb_signal_parts.append(f"转股溢价 {float(cb_premium_rate):+.2f}%")
                if cb_remain_size_yi == cb_remain_size_yi:
                    cb_signal_parts.append(f"余额约 {float(cb_remain_size_yi):.1f} 亿")
                factors.append(
                    _factor_row(
                        "可转债映射",
                        " / ".join(cb_signal_parts),
                        cb_award,
                        10,
                        cb_detail or "可转债基础/日线/技术因子已接入；这里只把发行人对应转债当作辅助信号，不把它写成确定性主线。",
                        factor_id="j4_convertible_bond_proxy",
                        factor_meta_overrides={
                            "source_as_of": cb_latest_date or None,
                            "degraded": False,
                        },
                    )
                )
            else:
                factors.append(
                    _factor_row(
                        "可转债映射",
                        cb_detail or str(convertible_bond_snapshot.get("disclosure") or "可转债快照非当期"),
                        0,
                        10,
                        str(convertible_bond_snapshot.get("disclosure") or "可转债基础信息非当期，不把缺口误写成映射已确认。"),
                        display_score="观察",
                        factor_id="j4_convertible_bond_proxy",
                        factor_meta_overrides={
                            "source_as_of": cb_latest_date or None,
                            "degraded": True,
                            "degraded_reason": "Tushare cb_basic / cb_daily / cb_factor_pro 非当期快照",
                        },
                    )
                )
        else:
            factors.append(
                _factor_row(
                    "可转债映射",
                    str(convertible_bond_snapshot.get("detail") or convertible_bond_snapshot.get("disclosure") or "可转债基础信息缺失"),
                    None,
                    10,
                    str(convertible_bond_snapshot.get("disclosure") or "可转债基础信息缺失，不把缺口误写成转债映射已确认。"),
                    display_score="观察",
                    factor_id="j4_convertible_bond_proxy",
                    factor_meta_overrides={
                        "degraded": True,
                        "degraded_reason": str(convertible_bond_snapshot.get("diagnosis", "missing")),
                    },
                )
            )

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
                pe_award = -12  # negative PE = loss-making
            elif pe_value < 15:
                pe_award = 25  # value zone
            elif pe_value < 25:
                pe_award = 20  # reasonable
            elif pe_value < 40:
                pe_award = 10  # growth premium
            elif pe_value < 60:
                pe_award = 0
            elif pe_value < 90:
                pe_award = -8
            else:
                pe_award = -15
            detail = f"个股滚动 PE {pe_value:.1f}x，直接按绝对水平评分。"
        else:
            if pe_percentile is not None and pe_percentile < 0.30:
                pe_award = 25
            elif pe_percentile is not None and pe_percentile < 0.50:
                pe_award = 10
            elif pe_percentile is not None and pe_percentile >= 0.90:
                pe_award = -15
            elif pe_percentile is not None and pe_percentile >= 0.75:
                pe_award = -8
            elif pe_value < 20:
                pe_award = 10
            elif pe_value >= 60:
                pe_award = -10
            else:
                pe_award = 0
            match_quality = str(valuation_snapshot.get("match_quality", "")).strip()
            if match_quality == "exact_holdings_proxy":
                detail = "估值库已命中精确基准，但直接滚动PE缺失；当前改用最近一期前五大重仓加权PE代理，价格位置另算，不与估值分位混用。"
            elif match_quality == "benchmark_holdings_proxy":
                detail = "估值库未直接命中精确宽基基准；当前改用最近一期前五大重仓加权PE代理，价格位置另算，不与估值分位混用。"
            elif match_quality == "exact":
                detail = "当前接入的是目标基准的滚动 PE；价格位置另算，不与估值分位混用。"
            elif match_quality == "theme_proxy":
                detail = "当前接入的是最接近主题指数的滚动 PE；价格位置另算，不与估值分位混用。"
            elif match_quality == "benchmark_no_proxy":
                detail = "估值库未直接命中精确宽基基准；为避免错配，不再回退到不相干主题代理。"
            else:
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
                factor_id="j4_pe_ttm",
            )
        )
    else:
        if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
            factors.append(
                _factor_row(
                    "估值代理分位",
                    f"价格位置代理 {price_percentile:.0%}",
                    None,
                    25,
                    "当前未接入可用指数估值；价格位置只保留展示，不再把高低位直接当成 ETF 基本面估值结论。",
                    display_score="观察",
                    factor_id="j4_valuation_proxy",
                )
            )
        else:
            percentile_award = 25 if price_percentile < 0.30 else 10 if price_percentile < 0.50 else -10 if price_percentile >= 0.90 else -5 if price_percentile >= 0.75 else 0
            raw += percentile_award
            available += 25
            factors.append(
                _factor_row(
                    "估值代理分位",
                    f"价格位置代理 {price_percentile:.0%}",
                    percentile_award,
                    25,
                    "当前未接入可用指数估值，只能用价格位置代理；价格分位不等于真实估值分位。",
                    factor_id="j4_valuation_proxy",
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
        revenue_val = float(revenue_yoy)
        revenue_award = 20 if revenue_val >= 20 else 15 if revenue_val >= 10 else 8 if revenue_val >= 5 else -8 if revenue_val < 0 else -4 if revenue_val < 3 else 0
        raw += revenue_award
        available += 20
        factors.append(
            _factor_row(
                "盈利增速",
                f"{revenue_label} {float(revenue_yoy):.1f}%",
                revenue_award,
                20,
                f"{proxy_scope_detail}，缺失时回退到利润同比；覆盖权重约 {financial_proxy.get('coverage_weight', 0.0):.1f}%。",
                factor_id="j4_revenue_growth",
            )
        )
    else:
        factors.append(_factor_row("盈利增速", "缺失", None, 20, "当前未接入对应指数/行业或重仓股的营收同比代理", factor_id="j4_revenue_growth"))

    roe_value = financial_proxy.get("roe")
    if roe_value is not None:
        roe_float = float(roe_value)
        roe_award = 20 if roe_float >= 15 else 10 if roe_float >= 10 else -8 if roe_float < 5 else -4 if roe_float < 8 else 0
        raw += roe_award
        available += 20
        factors.append(
            _factor_row(
                "ROE",
                f"{proxy_labels['roe']} {float(roe_value):.1f}%",
                roe_award,
                20,
                f"财务代理最新报告期 {financial_proxy.get('report_date') or '未知'}。",
                factor_id="j4_roe",
            )
        )
    else:
        factors.append(_factor_row("ROE", "缺失", None, 20, "当前未接入对应指数/行业或重仓股的 ROE 代理", factor_id="j4_roe"))

    gross_margin = financial_proxy.get("gross_margin")
    if gross_margin is not None:
        margin_float = float(gross_margin)
        margin_award = 15 if margin_float >= 30 else 10 if margin_float >= 20 else -6 if margin_float < 10 else -3 if margin_float < 15 else 0
        raw += margin_award
        available += 15
        factors.append(
            _factor_row(
                "毛利率",
                f"{proxy_labels['margin']} {float(gross_margin):.1f}%",
                margin_award,
                15,
                "用重仓股/成分股加权毛利率代理行业定价权和成本结构。",
                factor_id="j4_gross_margin",
            )
        )
    else:
        factors.append(_factor_row("毛利率", "缺失", None, 15, "当前未接入对应行业或重仓股毛利率代理", factor_id="j4_gross_margin"))

    profit_yoy = financial_proxy.get("profit_yoy")
    growth_base = None
    if profit_yoy is not None and float(profit_yoy) > 0:
        growth_base = float(profit_yoy)
    elif revenue_yoy is not None and float(revenue_yoy) > 0:
        growth_base = float(revenue_yoy)
    peg_value = float(pe_ttm) / growth_base if pe_ttm is not None and growth_base and growth_base > 0 else None
    if peg_value is not None:
        peg_award = 10 if peg_value < 1 else 5 if peg_value < 1.5 else -6 if peg_value >= 3 else -3 if peg_value >= 2 else 0
        raw += peg_award
        available += 10
        factors.append(
            _factor_row(
                "PEG 代理",
                f"PEG 约 {peg_value:.2f}",
                peg_award,
                10,
                f"用真实指数 PE 除以{proxy_labels['peg_base']}增速代理，回答'增长是否已经被定价'。",
                factor_id="j4_peg",
            )
        )
    else:
        factors.append(_factor_row("PEG 代理", "缺失", None, 10, "缺少稳定的盈利增速代理，未计算 PEG", factor_id="j4_peg"))

    # J-4: 补充财务质量因子（scoring_supportive / observation_only）
    # Hard constraints:
    #   - debt_to_assets / current_ratio 来自季报，存在 45 天 lag
    #   - cfps 代理现金流质量，季报 lag 相同
    #   - 盈利动量 (j4_earnings_momentum) 始终为 observation_only — 无可靠 point-in-time EPS 修正源
    if asset_type in {"cn_stock", "hk", "us"}:
        # J4.a: 经营现金流质量（j4_cashflow_quality, quarterly, lag 45d）
        cfps_val = financial_proxy.get("cfps")
        if cfps_val is not None:
            try:
                cfps = float(cfps_val)
                if cfps > 0:
                    cf_award = 10
                    cf_signal = f"每股经营现金流 {cfps:.2f}（现金流为正，盈利质量有支撑）"
                elif cfps >= -0.5:
                    cf_award = -2
                    cf_signal = f"每股经营现金流 {cfps:.2f}（轻微为负，需关注）"
                else:
                    cf_award = -6
                    cf_signal = f"每股经营现金流 {cfps:.2f}（明显为负，盈利质量存疑）"
                raw += cf_award
                available += 10
                factors.append(_factor_row(
                    "现金流质量", cf_signal, cf_award, 10,
                    f"每股经营现金流代理盈利现金含量；数据源：季报（T+45 天 lag），报告期 {financial_proxy.get('report_date') or '未知'}。",
                    factor_id="j4_cashflow_quality",
                ))
            except (ValueError, TypeError):
                factors.append(_factor_row("现金流质量", "数据解析异常", None, 10, "每股现金流数据解析失败。", factor_id="j4_cashflow_quality"))
        else:
            factors.append(_factor_row(
                "现金流质量", "缺失", None, 10,
                "经营现金流数据缺失；当前财务代理未覆盖 cfps，季报 T+45 天 lag。",
                factor_id="j4_cashflow_quality",
            ))

        # J4.b: 资产负债率 / 杠杆与偿债压力（j4_leverage, quarterly, lag 45d）
        debt_ratio = financial_proxy.get("debt_to_assets")
        current_ratio_val = financial_proxy.get("current_ratio")
        if debt_ratio is not None:
            try:
                dr = float(debt_ratio)
                if dr < 40:
                    lev_award = 10
                    lev_signal = f"资产负债率 {dr:.1f}%（低杠杆，财务稳健）"
                elif dr < 60:
                    lev_award = 6
                    lev_signal = f"资产负债率 {dr:.1f}%（中等杠杆）"
                elif dr < 80:
                    lev_award = -4
                    lev_signal = f"资产负债率 {dr:.1f}%（较高杠杆，需关注偿债压力）"
                else:
                    lev_award = -8
                    lev_signal = f"资产负债率 {dr:.1f}%（高杠杆警戒，偿债风险高）"
                extra = f"；流动比率 {float(current_ratio_val):.2f}" if current_ratio_val is not None else ""
                raw += lev_award
                available += 10
                factors.append(_factor_row(
                    "杠杆压力", f"{lev_signal}{extra}", lev_award, 10,
                    f"资产负债率代理财务杠杆水平；数据源：季报（T+45 天 lag），报告期 {financial_proxy.get('report_date') or '未知'}。",
                    factor_id="j4_leverage",
                ))
            except (ValueError, TypeError):
                factors.append(_factor_row("杠杆压力", "数据解析异常", None, 10, "资产负债率数据解析失败。", factor_id="j4_leverage"))
        else:
            factors.append(_factor_row(
                "杠杆压力", "缺失", None, 10,
                "资产负债率数据缺失；当前财务代理未覆盖 debt_to_assets，季报 T+45 天 lag。",
                factor_id="j4_leverage",
            ))

        # J4.c: 盈利动量 / EPS 修正代理（j4_earnings_momentum, observation_only）
        # Hard constraint: 无可靠 point-in-time EPS 修正源，始终为 observation_only。
        # 当前以扣非净利同比加速/减速作为动量方向的弱代理。
        profit_dedt = financial_proxy.get("profit_dedt_yoy")
        profit_base = financial_proxy.get("profit_yoy")
        if profit_dedt is not None and profit_base is not None:
            try:
                momentum_signal = "加速" if float(profit_dedt) > float(profit_base) else "减速"
                ep_signal = f"扣非净利同比 {float(profit_dedt):.1f}% vs 净利同比 {float(profit_base):.1f}%（{momentum_signal}代理）"
            except (ValueError, TypeError):
                ep_signal = "盈利动量数据解析异常"
        elif profit_base is not None:
            try:
                ep_signal = f"净利同比 {float(profit_base):.1f}%（扣非缺失，单一数据点不构成动量信号）"
            except (ValueError, TypeError):
                ep_signal = "盈利动量代理数据解析异常"
        else:
            ep_signal = "盈利动量代理数据缺失"
        factors.append(_factor_row(
            "盈利动量", ep_signal, 0, 0,
            "j4_earnings_momentum 在无可靠 point-in-time EPS 修正源时始终为 observation_only，不进入评分。当前以扣非/净利同比差异作为方向弱代理，仅供参考。",
            display_score="观察提示",
            factor_id="j4_earnings_momentum",
            factor_meta_overrides={
                "degraded": True,
                "degraded_reason": "缺少可靠 point-in-time EPS 修正源和 lag fixture，当前只允许作为观察提示。",
            },
        ))

    flow_award: Optional[int] = None
    flow_detail = "ETF 份额 / 行业资金流代理暂缺"
    flow_signal = "ETF 份额 / 资金流向"
    flow_display_score: Optional[str] = None
    if asset_type == "cn_etf" and not is_light_etf_profile:
        try:
            fund_flow = ChinaMarketCollector(config).get_etf_fund_flow(symbol)
            flow_series = pd.to_numeric(fund_flow.get("净流入", pd.Series(dtype=float)), errors="coerce").dropna()
            if flow_series.empty and "净申购份额" in fund_flow.columns:
                flow_series = pd.to_numeric(fund_flow["净申购份额"], errors="coerce").dropna()
            if not flow_series.empty:
                tail_series = flow_series.tail(5)
                positive_days = int((tail_series > 0).sum())
                negative_days = int((tail_series < 0).sum())
                flow_award = 10 if positive_days >= 3 else -5 if negative_days >= 3 else 0
                flow_detail = f"近 5 个可用样本中 {positive_days} 个为净流入/净申购，{negative_days} 个为净流出/净赎回"
                flow_signal = "ETF 份额近 5 个样本有承接" if flow_award > 0 else "ETF 近 5 个样本持续净流出" if flow_award < 0 else "ETF 流入承接不稳"
        except Exception as exc:
            flow_detail = f"ETF 份额数据缺失: {exc}"
    if flow_award is None and sector_flow:
        main_flow = sector_flow.get("main_flow")
        main_ratio = sector_flow.get("main_ratio")
        flow_award = 10 if main_flow is not None and float(main_flow) > 0 else -5 if main_flow is not None and float(main_flow) < 0 else 0
        flow_signal = f"{sector_flow.get('name') or metadata.get('sector', '行业')} 主力净{'流入' if (main_flow or 0) > 0 else '流出'} {_fmt_yi_number(main_flow)}"
        flow_detail = (
            f"ETF 份额缺失，改用行业资金流代理；主力净占比 "
            f"{format_pct(float(main_ratio) / 100) if main_ratio is not None and abs(float(main_ratio)) > 1 else format_pct(float(main_ratio)) if main_ratio is not None else '缺失'}。"
        )
    elif asset_type == "cn_etf" and flow_award is None:
        flow_award = -2
        flow_signal = "ETF 份额 / 资金流向缺失"
        if is_light_etf_profile:
            flow_detail = "ETF discovery 轻量模式当前跳过份额申赎慢链；若行业资金流也缺失，则先按轻度保守分处理。"
        else:
            flow_detail = "ETF 份额与行业资金流代理都缺失；为避免因缺数据而抬高基本面分，这里保守按轻度负分处理。"
        flow_display_score = "-2/10（缺失保守）"
    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        flow_display_score = "信息项" if (flow_award or 0) > 0 else "观察"
        flow_detail = f"{flow_detail} 这条更适合放在筹码/相对强弱层，不再计入 ETF/基金 的基本面主分。"
        factors.append(_factor_row("资金承接", flow_signal, None, 10, flow_detail, display_score=flow_display_score))
    else:
        factors.append(_factor_row("资金承接", flow_signal, flow_award, 10, flow_detail, display_score=flow_display_score))
    if flow_award is not None and asset_type not in {"cn_etf", "cn_index", "cn_fund"}:
        raw += flow_award
        available += 10

    # J-5: ETF / 基金专属因子（成分集中度、主题纯度、跟踪基准、经理稳定性、费率等）
    j5_factors, j5_raw, j5_available = _j5_etf_fund_factors(asset_type, metadata, fund_profile)
    factors.extend(j5_factors)
    raw += j5_raw
    available += j5_available

    score = _normalize_dimension(raw, available, 100)
    # When data coverage is very low (proxy-only, e.g. HK/US stocks with no PE/ROE data),
    # cap the score. With only price-percentile proxy (available=25), normalization would
    # give 100/100 which severely distorts rankings. Cap at 55 to keep it below the "strong
    # fundamental" threshold used in rating logic.
    if score is not None and available < 35:
        score = min(score, 55)
    if asset_type in {"cn_etf", "cn_index", "cn_fund"} and score is not None:
        proxy_weight = float(financial_proxy.get("coverage_weight") or 0.0)
        if valuation_snapshot is None and proxy_weight < 35:
            score = min(score, 68)
    is_single_stock = asset_type in {"cn_stock", "hk", "us"}
    negative_summary = "当前估值与财务质量都未形成明显优势。"
    if is_single_stock:
        if pe_ttm is None and available < 35:
            negative_summary = "当前财务覆盖不足，先不下明确估值结论。"
        elif pe_ttm is not None and float(pe_ttm) <= 15:
            negative_summary = "估值不贵，但财务质量或盈利确认还不足以支撑更高基本面分。"
        elif pe_ttm is not None and float(pe_ttm) >= 30:
            negative_summary = "当前估值不便宜，且财务质量安全边际不够厚。"
        else:
            negative_summary = "当前估值与财务质量都未形成明显优势。"
    summary = _dimension_summary(
        score,
        "个股估值/财务快照偏正面，基本面支撑存在。" if is_single_stock else "产品结构和基本面代理偏正面，但这不等于底层行业景气已经被直接验证。",
        "个股基本面暂无明显低估或高估结论。" if is_single_stock else "产品结构和基本面代理大体中性，当前还不能把它解释成底层行业明显低估。",
        negative_summary if is_single_stock else "估值代理或产品承接一般，当前安全边际不够厚。",
        "ℹ️ 个股基本面数据缺失，本次评级未纳入完整基本面维度" if is_single_stock else "ℹ️ 产品结构/基本面代理数据缺失，本次评级未纳入完整维度",
    )
    if score is not None and available < 35:
        summary += " 当前仅基于代理因子归一化评分。"
    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        summary += " 当前分数更接近产品质量、跟踪机制和主题代理，不直接等同于底层行业基本面已经确认。"
    if valuation_snapshot and pe_ttm is not None:
        summary += (
            f" 当前已接入 `{valuation_snapshot.get('index_name', '')}` "
            f"{valuation_snapshot.get('metric_label', '滚动PE')} {float(pe_ttm):.1f}x；{valuation_note}"
        )
    else:
        summary += f" {valuation_note}"
    return {
        "name": "基本面",
        "display_name": display_name,
        "score": score,
        "max_score": 100,
        "summary": summary,
        "factors": factors,
        "core_signal": _top_material_signals(factors),
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
    if (
        asset_type_str == "cn_stock"
        and news_mode != "proxy"
        and not _runtime_feature_disabled(context, "skip_cn_stock_direct_news_runtime")
    ):
        stock_news_items = _context_stock_news(str(metadata.get("symbol", "")), context)
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
    dynamic_search_attempted = False
    dynamic_search_groups = _catalyst_search_groups(metadata, profile)
    attempted_search_groups = list(dynamic_search_groups)
    dynamic_search_enabled = bool(config.get("news_topic_search_enabled", True)) and not bool(
        config.get("skip_catalyst_dynamic_search_runtime", False)
    )
    if (
        dynamic_search_enabled
        and dynamic_search_groups
        and
        len(strict_related_news) + len(category_related_news) < 2
        and not commodity_like_fund
        and not calendar_forward_events
        and not stock_news_items
        and news_mode in {"live", "proxy"}
    ):
        dynamic_search_attempted = True
        if asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and news_mode == "proxy":
            theme_hint = any(str(item).strip() for item in metadata.get("chain_nodes", []) if str(item).strip())
            attempted_search_groups = dynamic_search_groups[:4] if theme_hint else dynamic_search_groups[:2]
        try:
            collector = NewsCollector(config)
            dynamic_related_news = collector.search_by_keyword_groups(
                attempted_search_groups,
                preferred_sources=_preferred_catalyst_sources(metadata, profile),
                limit=4 if asset_type_str in {"cn_etf", "cn_fund", "cn_index"} else 6,
                recent_days=7,
            )
            if not dynamic_related_news:
                dynamic_related_news = collector.search_by_keywords(
                    _catalyst_search_terms(metadata, profile),
                    preferred_sources=_preferred_catalyst_sources(metadata, profile),
                    limit=4 if asset_type_str in {"cn_etf", "cn_fund", "cn_index"} else 6,
                    recent_days=7,
                )
        except Exception:
            dynamic_related_news = []
    all_news_pool = _dedupe_news_items([*strict_related_news, *category_related_news, *dynamic_related_news])

    # For cn_stock: inject per-stock news from akshare (东方财富个股新闻).
    if asset_type_str == "cn_stock":
        if stock_news_items:
            all_news_pool = _dedupe_news_items([*all_news_pool, *stock_news_items])
    fresh_news_pool = _filter_fresh_intelligence(all_news_pool, reference_now)
    stale_news_pool = [item for item in all_news_pool if item not in fresh_news_pool]
    recent_theme_news_pool = [
        item
        for item in all_news_pool
        if _is_within_lookback(item, reference_now, lookback_days=ETF_THEME_CATALYST_LOOKBACK_DAYS)
    ]

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
    instrument_identity_tokens = (
        _instrument_identity_tokens(metadata)
        if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}
        else []
    )
    catalyst_maxima = _catalyst_factor_maxima(
        profile,
        asset_type=asset_type_str,
        is_individual_stock=bool(stock_name_tokens),
    )
    if stock_name_tokens:
        stock_specific_pool_all = [
            item for item in all_news_pool if _contains_any(_headline_text(item), stock_name_tokens)
        ]
        stock_specific_pool = [
            item for item in fresh_news_pool if _contains_any(_headline_text(item), stock_name_tokens)
        ]
    elif instrument_identity_tokens:
        stock_specific_pool_all = _instrument_specific_news_items(all_news_pool, metadata)
        stock_specific_pool = _instrument_specific_news_items(fresh_news_pool, metadata)
    else:
        stock_specific_pool_all = all_news_pool
        stock_specific_pool = fresh_news_pool
    existing_forward_events = _dedupe_news_items(
        [
            *calendar_forward_events,
            *_company_forward_events(
                metadata,
                context,
                news_items=stock_specific_pool_all if stock_specific_pool_all else all_news_pool,
            ),
        ]
    )
    # For HK/US individual stocks: proactively search direct company-event headlines when
    # broad market news does not already produce high-confidence company evidence.
    company_positive_pool = stock_specific_pool
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and instrument_identity_tokens:
        company_positive_pool = [
            item
            for item in stock_specific_pool_all
            if _is_high_confidence_company_news(item)
            and _is_within_lookback(item, reference_now, lookback_days=DIRECT_COMPANY_NEWS_LOOKBACK_DAYS)
        ]
    elif asset_type_str in {"hk", "us"} and stock_name_tokens:
        company_positive_pool = [
            item
            for item in stock_specific_pool_all
            if _is_high_confidence_company_news(item)
            and _is_within_lookback(item, reference_now, lookback_days=DIRECT_COMPANY_NEWS_LOOKBACK_DAYS)
        ]
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
                all_news_pool = _dedupe_news_items([*all_news_pool, *hk_us_news])
                fresh_news_pool = _filter_fresh_intelligence(all_news_pool, reference_now)
                stale_news_pool = [item for item in all_news_pool if item not in fresh_news_pool]
                stock_specific_pool_all = [item for item in all_news_pool if _contains_any(_headline_text(item), stock_name_tokens)]
                stock_specific_pool = [item for item in fresh_news_pool if _contains_any(_headline_text(item), stock_name_tokens)]
                company_positive_pool = [
                    item
                    for item in stock_specific_pool_all
                    if _is_high_confidence_company_news(item)
                    and _is_within_lookback(item, reference_now, lookback_days=DIRECT_COMPANY_NEWS_LOOKBACK_DAYS)
                ]
    company_specific_news_available = (
        bool(company_positive_pool)
        if (
            (asset_type_str in {"hk", "us"} and stock_name_tokens)
            or (asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and instrument_identity_tokens)
        )
        else True
    )

    policy_items = [
        item
        for item in fresh_news_pool
        if (
            str(item.get("category", "")).lower() in {"china_macro", "china_macro_domestic"}
            or str(item.get("source", "")).strip() in {"财联社", "证券时报", "Reuters"}
        )
        and _contains_any(_headline_text(item), policy_keys)
        and _contains_any(_headline_text(item), strict_tokens)
    ]
    policy_pick = _pick_best_news_item(policy_items, policy_keys, keyword_keys, reference_time=reference_now)
    if asset_type_str in {"hk", "us"} and stock_name_tokens:
        specific_policy_items = [item for item in policy_items if item in company_positive_pool]
        policy_pick = _pick_best_news_item(
            specific_policy_items,
            policy_keys,
            stock_name_tokens or keyword_keys,
            reference_time=reference_now,
        )
        policy_award = 10 if specific_policy_items else 0
    elif is_individual_stock and stock_name_tokens:
        # cn_stock: full 30pts only when the policy news names the company directly;
        # sector-level policy (e.g. industry-wide AI/tech support) gets only 10pts.
        specific_policy_items = [item for item in policy_items if _contains_any(_headline_text(item), stock_name_tokens)]
        policy_award = 30 if specific_policy_items else (10 if policy_items else 0)
    elif asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        specific_policy_items = [item for item in policy_items if item in company_positive_pool]
        policy_pick = _pick_best_news_item(
            specific_policy_items,
            instrument_identity_tokens or keyword_keys,
            keyword_keys,
            reference_time=reference_now,
        )
        policy_award = 10 if specific_policy_items else 0
    else:
        policy_award = 30 if policy_items else 0
    if asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available:
        policy_pick = None
        policy_award = 0
    # For cn_stock with per-stock news: redistribute weights (policy 25, leader 15, new factor 15)
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        _policy_default_max = 10
    else:
        _policy_default_max = 25 if (asset_type_str == "cn_stock" and stock_name_tokens) else 30
    _policy_max = catalyst_maxima.get("policy", _policy_default_max)
    policy_award = min(policy_award, _policy_default_max)
    policy_award = _rescale_catalyst_award(policy_award, _policy_default_max, _policy_max)
    raw += policy_award
    available += _policy_max
    if asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available:
        policy_signal = "未命中高置信个股直连新闻，个股催化暂不计分"
        policy_detail = "当前未命中 Reuters/Bloomberg/FT/公司公告 这类高置信个股直连标题，避免把市场级新闻误记成个股催化。"
    elif asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        policy_signal = policy_pick["title"] if policy_award > 0 and policy_pick else "产品级直接政策情报偏弱"
        policy_detail = "对 ETF/基金/指数，这里只把产品自身或官方直连政策/公告算进政策催化，不把同赛道热度或 peer ETF 消息直接当成产品催化。"
    else:
        policy_signal = policy_pick["title"] if policy_award > 0 and policy_pick else "近 7 日直接政策情报偏弱"
        policy_detail = "政策原文和一级媒体优先"
    factors.append(_factor_row("政策催化", policy_signal, policy_award, _policy_max, policy_detail))
    if policy_award > 0 and policy_pick:
        evidence_rows.append(_evidence_row(layer="政策催化", item=policy_pick))

    leader_items = [
        item
        for item in fresh_news_pool
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
    leader_pick = _pick_best_news_item(
        leader_items,
        [*domestic_leaders, *strict_event_keys],
        keyword_keys,
        reference_time=reference_now,
    )
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
            leader_pick = _pick_best_news_item(
                stock_specific_leader_items,
                [*keyword_keys, *strict_event_keys],
                stock_name_tokens or keyword_keys,
                reference_time=reference_now,
            )
    product_specific_leader_items: List[Mapping[str, Any]] = []
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        product_specific_leader_items = [item for item in leader_items if item in company_positive_pool]
        if product_specific_leader_items:
            leader_pick = _pick_best_news_item(
                product_specific_leader_items,
                instrument_identity_tokens or keyword_keys,
                keyword_keys,
                reference_time=reference_now,
            )
        else:
            leader_pick = None
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        _leader_default_max = 10
    else:
        _leader_default_max = 15 if (asset_type_str == "cn_stock" and stock_name_tokens) else 25
    _leader_max = catalyst_maxima.get("leader", _leader_default_max)
    if asset_type_str in {"hk", "us"} and stock_name_tokens:
        leader_award = _leader_default_max if stock_specific_leader_items else 0
        if not company_specific_news_available:
            leader_pick = None
    elif asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        leader_award = _leader_default_max if product_specific_leader_items else 0
    elif asset_type_str == "cn_stock" and stock_name_tokens:
        leader_award = _leader_default_max if stock_specific_leader_items else 0
        if not stock_specific_leader_items:
            leader_pick = None
    else:
        leader_award = _leader_default_max if (leader_items or stock_specific_leader_items) else 0
    leader_award = _rescale_catalyst_award(leader_award, _leader_default_max, _leader_max)
    raw += leader_award
    available += _leader_max
    if asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available:
        leader_signal = "未命中高置信个股直连新闻，个股催化暂不计分"
        leader_detail = "当前未命中 Reuters/Bloomberg/FT/公司公告 这类高置信业绩/公告标题，避免把行业级消息误映射到单一个股。"
    elif asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        leader_signal = leader_pick["title"] if leader_award > 0 and leader_pick else "产品级直接业绩/公告情报偏弱"
        leader_detail = "对 ETF/基金/指数，这里只认产品自身或官方直连产品情报，不把同赛道产品涨跌、申购或媒体热度直接上翻成产品级业绩催化。"
    else:
        leader_signal = leader_pick["title"] if leader_award > 0 and leader_pick else "直接龙头公告/业绩情报偏弱"
        leader_detail = "优先看订单、扩产、回购、并购或超预期业绩"
    factors.append(_factor_row("龙头公告/业绩", leader_signal, leader_award, _leader_max, leader_detail))
    if leader_award > 0 and leader_pick:
        evidence_rows.append(_evidence_row(layer="龙头公告/业绩", item=leader_pick))

    structured_news_items = (
        company_positive_pool
        if (
            (asset_type_str in {"hk", "us"} and stock_name_tokens)
            or (asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and instrument_identity_tokens)
        )
        else (stock_specific_pool_all if stock_specific_pool_all else all_news_pool)
    )
    structured_event_pool = _structured_company_event_items(
        metadata,
        context,
        news_items=structured_news_items,
        stock_news_items=stock_news_items,
    )
    structured_non_negative_pool = [
        item
        for item in structured_event_pool
        if not _is_non_positive_company_statement(item)
    ]
    structured_direct_pool = [
        item
        for item in structured_non_negative_pool
        if str(item.get("category", "")).lower() not in {"earnings_calendar", "stock_disclosure_calendar"}
    ]
    structured_pick = _pick_best_structured_item(
        structured_direct_pool or structured_non_negative_pool,
        [*earnings_keys, *event_keys, *STRUCTURED_COMPANY_EVENT_KEYS],
        stock_name_tokens or keyword_keys,
        reference_now,
    )
    structured_direct_keys = {
        (str(item.get("title", "")).strip(), str(item.get("source", "")).strip())
        for item in structured_direct_pool
    }
    is_direct_structured = bool(
        structured_pick
        and (str(structured_pick.get("title", "")).strip(), str(structured_pick.get("source", "")).strip()) in structured_direct_keys
    )
    is_ir_structured = bool(structured_pick and _is_ir_interaction_item(structured_pick))
    if structured_pick:
        structured_award, freshness_detail = _structured_event_award(
            structured_pick,
            reference_now,
            strong_event=is_direct_structured and not is_ir_structured,
        )
        if structured_award > 0 and is_ir_structured:
            structured_detail = f"当前命中互动平台/投资者关系口径，先按补充证据处理，不替代订单、财报或正式公告。{freshness_detail}"
        elif structured_award > 0 and is_direct_structured:
            structured_detail = f"先看公告、财报、订单、回购、合作这类结构化公司事件；这类证据比泛行业新闻更接近可执行催化。{freshness_detail}"
        elif structured_award > 0:
            structured_detail = f"当前只命中财报日历/披露窗口等结构化事件，属于催化线索已出现，但还没到强催化共识。{freshness_detail}"
        else:
            structured_detail = freshness_detail
    else:
        structured_award = 0
        structured_detail = "当前未命中结构化公司事件；这里按信息不足处理，不直接等于个股没有催化。"
    _structured_max = catalyst_maxima.get("structured", 15)
    structured_award = _rescale_catalyst_award(structured_award, 15, _structured_max)
    effective_structured_event = bool(structured_pick and structured_award > 0)
    raw += structured_award
    available += _structured_max
    factors.append(
        _factor_row(
            "结构化事件",
            structured_pick["title"] if structured_pick else "未命中明确结构化公司事件",
            structured_award,
            _structured_max,
            structured_detail,
        )
    )
    if structured_award > 0 and structured_pick:
        evidence_rows.append(_evidence_row(layer="结构化事件", item=structured_pick))

    if asset_type_str == "cn_stock":
        broker_snapshot = _cn_stock_broker_recommend_snapshot(metadata, context)
        broker_status = str(broker_snapshot.get("status", "")).strip()
        broker_count = int(broker_snapshot.get("latest_broker_count") or 0)
        broker_delta = broker_snapshot.get("broker_delta")
        broker_detail = str(broker_snapshot.get("detail", "")).strip() or str(broker_snapshot.get("disclosure", "")).strip()
        if bool(broker_snapshot.get("is_fresh")) and broker_status == "matched" and broker_count >= 2:
            if broker_delta is not None and float(broker_delta) > 0:
                broker_award = 8 if broker_count >= 5 or float(broker_delta) >= 2 else 5
                broker_signal = f"本月 {broker_count} 家券商金股推荐，较上月增加 {int(broker_delta)} 家"
            elif broker_count >= 4:
                broker_award = 5
                broker_signal = f"本月 {broker_count} 家券商金股推荐，卖方覆盖维持高位"
            else:
                broker_award = 3
                broker_signal = f"本月 {broker_count} 家券商金股推荐"
            raw += broker_award
            available += 8
            factors.append(
                _factor_row(
                    "卖方覆盖/一致预期",
                    broker_signal,
                    broker_award,
                    8,
                    broker_detail or "券商月度金股覆盖只当卖方共识热度参考，不替代公司公告、订单或财报催化。",
                )
            )
        elif broker_status == "stale":
            available += 8
            factors.append(
                _factor_row(
                    "卖方覆盖/一致预期",
                    f"最新卖方金股仍停在 {str(broker_snapshot.get('latest_date', '')).strip() or '历史月份'}",
                    0,
                    8,
                    broker_detail or "卖方月度金股当前只命中历史月份，本轮不把旧共识写成本月 fresh 卖方升温。",
                    display_score="观察",
                )
            )
        elif broker_status == "empty":
            available += 8
            factors.append(
                _factor_row(
                    "卖方覆盖/一致预期",
                    "本月未命中明确券商金股推荐",
                    0,
                    8,
                    broker_detail or "当前月份未命中券商月度金股名单，不把空结果误写成卖方已经明确看空。",
                    display_score="信息项",
                )
            )
        else:
            factors.append(
                _factor_row(
                    "卖方覆盖/一致预期",
                    "缺失",
                    None,
                    8,
                    broker_detail or "当前未拿到可稳定使用的券商月度金股专题，不把缺口误写成零覆盖。",
                )
            )

        board_action_snapshot = _cn_stock_board_action_snapshot(metadata, context)
        board_detail = str(board_action_snapshot.get("detail", "")).strip() or "打板专题当前未提供有效结论。"
        if bool(board_action_snapshot.get("is_fresh")) and bool(board_action_snapshot.get("has_positive_signal")):
            board_positive_bits = [str(item).strip() for item in list(board_action_snapshot.get("positive_bits") or []) if str(item).strip()]
            board_award = 12 if str(board_action_snapshot.get("lhb_reason", "")).strip() or bool(board_action_snapshot.get("in_strong_pool")) else 6
            raw += board_award
            available += 12
            factors.append(
                _factor_row(
                    "龙虎榜/打板确认",
                    " / ".join(board_positive_bits[:2]) or "龙虎榜/打板结构偏正面",
                    board_award,
                    12,
                    board_detail + " 这类微观交易结构只当短线催化/风险偏好确认，不替代基本面与产业逻辑。",
                )
            )
        elif bool(board_action_snapshot.get("is_fresh")) and bool(board_action_snapshot.get("has_negative_signal")):
            board_negative_bits = [str(item).strip() for item in list(board_action_snapshot.get("negative_bits") or []) if str(item).strip()]
            board_penalty = 6 if bool(board_action_snapshot.get("in_dt_pool")) else 4
            raw -= board_penalty
            available += 12
            factors.append(
                _factor_row(
                    "龙虎榜/打板确认",
                    " / ".join(board_negative_bits[:2]) or "龙虎榜/打板结构偏负面",
                    -board_penalty,
                    12,
                    board_detail + " 微观交易结构转弱时，短线催化更容易先被情绪风险打断。",
                    display_score=f"-{board_penalty}",
                )
            )
        elif board_action_snapshot:
            available += 12
            factors.append(
                _factor_row(
                    "龙虎榜/打板确认",
                    "未命中明确龙虎榜/打板确认",
                    0,
                    12,
                    board_detail or "当前未命中可稳定使用的龙虎榜/打板专题信号。",
                    display_score="信息项",
                )
            )
        else:
            factors.append(
                _factor_row(
                    "龙虎榜/打板确认",
                    "缺失",
                    None,
                    12,
                    "当前未拿到可稳定使用的龙虎榜/竞价/涨跌停专题，不把缺口误写成没有短线催化。",
                )
            )

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
                    -penalty,
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

    themed_negative_items = [
        item
        for item in all_news_pool
        if _contains_any(_title_source_text(item), NEGATIVE_THEME_HEADWIND_KEYS)
        and (
            _contains_any(_title_source_text(item), [*keyword_keys, *strict_event_keys])
            or _contains_any(_title_source_text(item), [sector, *profile.get("themes", [])])
        )
    ]
    theme_negative_pick = _pick_best_news_item(
        themed_negative_items,
        [*NEGATIVE_THEME_HEADWIND_KEYS, *keyword_keys, *strict_event_keys],
        [sector, *profile.get("themes", [])],
        reference_time=reference_now,
    )
    theme_negative_penalty = 10 if theme_negative_pick else 0
    raw -= theme_negative_penalty
    available += 10
    if theme_negative_pick:
        factors.append(
            _factor_row(
                "主题逆风",
                str(theme_negative_pick.get("title", "")).strip(),
                -theme_negative_penalty,
                10,
                "这里识别的是主题/产业链层面的逆风，不等于单一公司基本面失效；但会拖慢催化从故事走向价格确认。",
                display_score=f"-{theme_negative_penalty}",
            )
        )
        evidence_rows.append(_evidence_row(layer="主题逆风", item=theme_negative_pick))
    else:
        factors.append(
            _factor_row(
                "主题逆风",
                "近 30 日未命中明确主题/产业链逆风头条",
                0,
                10,
                "当前未识别到会明显压制这条主题风险偏好的产业链级逆风新闻。",
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
        for item in fresh_news_pool
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
    overseas_pick = _pick_best_news_item(
        overseas_items,
        [*overseas_leaders, *earnings_keys, *strict_event_keys],
        [*keyword_keys, *overseas_keyword_map.get(sector, [])],
        reference_time=reference_now,
    )
    _overseas_max = catalyst_maxima.get("overseas", 20)
    overseas_award = 20 if overseas_items else 0
    if asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available:
        overseas_pick = None
        overseas_award = 0
    overseas_award = _rescale_catalyst_award(overseas_award, 20, _overseas_max)
    raw += overseas_award
    available += _overseas_max
    overseas_signal = (
        "未命中高置信个股直连新闻，海外映射暂不计分"
        if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available)
        else (overseas_pick["title"] if overseas_award > 0 and overseas_pick else "直接海外映射情报偏弱")
    )
    overseas_detail = "当前未命中与公司直接相关的高置信海外映射新闻，避免把行业级海外消息直接算成个股催化。" if (asset_type_str in {"hk", "us"} and stock_name_tokens and not company_specific_news_available) else "重点看海外龙头财报/指引或模型产品催化"
    factors.append(_factor_row("海外映射", overseas_signal, overseas_award, _overseas_max, overseas_detail))
    if overseas_award > 0 and overseas_pick:
        evidence_rows.append(_evidence_row(layer="海外映射", item=overseas_pick))

    if asset_type_str in {"cn_etf", "cn_fund"}:
        directional_snapshot = _select_fund_directional_news_pool(
            recent_theme_news_pool,
            dynamic_related_news,
            all_news_pool,
            fund_profile,
            metadata=metadata,
            profile=profile,
        )
        directional_award = int(directional_snapshot.get("award", 0) or 0)
        directional_item = directional_snapshot.get("item")
        matched_groups = list(directional_snapshot.get("matched_groups") or [])
        matched_terms = list(directional_snapshot.get("matched_terms") or [])
        fallback_scope = str(directional_snapshot.get("fallback_scope", "")).strip()
        raw += directional_award
        available += 12
        if directional_award > 0 and directional_item:
            detail = (
                f"优先看跟踪基准、行业暴露和核心成分的共振，而不是把泛主题热词直接当成 ETF 催化。"
                f" 当前命中 `{ ' / '.join(matched_groups) }`，关键词 `{ ' / '.join(matched_terms[:3]) }`。"
            )
            if fallback_scope == "dynamic_related_news":
                detail += " 近 7 日最新池先被融资/成交类泛 ETF 杂讯占住，本次直接回退到动态主题搜索命中做方向识别。"
            elif fallback_scope == "all_news_pool":
                detail += " 近 7 日最新池先被融资/成交类泛 ETF 杂讯占住，本次已回退到完整主题情报池做方向识别。"
            factors.append(
                _factor_row(
                    "产品/跟踪方向催化",
                    str(directional_item.get("title", "")).strip(),
                    directional_award,
                    12,
                    detail,
                    factor_id="j5_directional_catalyst",
                )
            )
            evidence_rows.append(_evidence_row(layer="产品/跟踪方向催化", item=directional_item))
        else:
            factors.append(
                _factor_row(
                    "产品/跟踪方向催化",
                    "近 7 日未命中跟踪基准/行业暴露/核心成分共振催化",
                    0,
                    12,
                    "这里优先看跟踪基准、行业暴露和核心成分的直接共振，不把泛主题新闻直接算成 ETF/基金催化。",
                    display_score="信息项",
                    factor_id="j5_directional_catalyst",
                )
            )

    # For individual stocks: density and heat only count articles that directly mention the stock.
    # This prevents sector-level news (e.g. broad AI/tech news) from inflating density scores.
    density_pool = company_positive_pool if (asset_type_str in {"hk", "us"} and stock_name_tokens) else (stock_specific_pool if (is_individual_stock and stock_name_tokens) else fresh_news_pool)
    fresh_directional_pool: List[Mapping[str, Any]] = []
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        fresh_directional_pool = [
            item
            for item in fresh_news_pool
            if _fund_directional_catalyst_signal(
                [item],
                fund_profile,
                metadata=metadata,
                profile=profile,
            )
        ]
        etf_density_pool = [*fresh_directional_pool]
        if not etf_density_pool and recent_theme_news_pool:
            etf_density_pool = [
                item
                for item in recent_theme_news_pool
                if _fund_directional_catalyst_signal(
                    [item],
                    fund_profile,
                    metadata=metadata,
                    profile=profile,
                )
            ]
        density_pool = etf_density_pool
    density_count = len(density_pool)
    density_label = (
        f"个股新增直接情报 {density_count} 条（主题/行业情报 {len(fresh_news_pool)} 条）"
        if (is_individual_stock and stock_name_tokens)
        else f"新增情报 {len(fresh_news_pool)} 条"
    )
    _density_max = catalyst_maxima.get("news_density", 10)
    density_award = 10 if density_count >= 2 else (5 if density_count >= 1 else 0)
    density_award = _rescale_catalyst_award(density_award, 10, _density_max)
    raw += density_award
    available += _density_max
    factors.append(_factor_row("研报/新闻密度", density_label, density_award, _density_max, "这里优先统计近 3 日新增情报，不把旧闻回放直接算成新催化。"))

    heat_pool = company_positive_pool if (asset_type_str in {"hk", "us"} and stock_name_tokens) else (stock_specific_pool if (is_individual_stock and stock_name_tokens) else fresh_news_pool)
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        etf_heat_pool = [
            item
            for item in fresh_news_pool
            if _fund_directional_catalyst_signal(
                [item],
                fund_profile,
                metadata=metadata,
                profile=profile,
            )
        ]
        if not etf_heat_pool and recent_theme_news_pool:
            etf_heat_pool = [
                item
                for item in recent_theme_news_pool
                if _fund_directional_catalyst_signal(
                    [item],
                    fund_profile,
                    metadata=metadata,
                    profile=profile,
                )
            ]
        heat_pool = etf_heat_pool
    source_count = len({str(item.get("source", "")) for item in heat_pool if item.get("source")})
    _heat_max = catalyst_maxima.get("news_heat", 10)
    heat_award = 10 if source_count >= 2 else 0
    heat_award = _rescale_catalyst_award(heat_award, 10, _heat_max)
    raw += heat_award
    available += _heat_max
    heat_signal = f"覆盖源 {source_count} 个" if source_count >= 1 else "情报覆盖偏窄"
    factors.append(_factor_row("新闻热度", heat_signal, heat_award, _heat_max, "从少量提及到多源同步，是热度拐点的代理"))
    if density_award > 0 or heat_award > 0:
        existing_titles = {str(item.get("title", "")).strip() for item in evidence_rows if str(item.get("title", "")).strip()}
        for item in _dedupe_news_items(list(heat_pool))[:2]:
            title = str(item.get("title", "")).strip()
            if not title or title in existing_titles:
                continue
            evidence_rows.append(_evidence_row(layer="新闻热度", item=item))
            existing_titles.add(title)

    forward_events = _dedupe_news_items([*related_events, *existing_forward_events])
    _forward_max = catalyst_maxima.get("forward_event", 5)
    forward_award = 5 if forward_events else 0
    forward_award = _rescale_catalyst_award(forward_award, 5, _forward_max)
    raw += forward_award
    available += _forward_max
    factors.append(
        _factor_row(
            "前瞻催化",
            forward_events[0]["title"] if forward_events else f"未来 {FORWARD_EVENT_LOOKAHEAD_DAYS} 日前瞻催化窗口暂不突出",
            forward_award,
            _forward_max,
            "未来财报/发布会/事件窗口已纳入；HK/US 个股优先读取公司级财报日历。",
        )
    )
    if forward_award > 0 and forward_events:
        evidence_rows.append(_evidence_row(layer="前瞻催化", item=forward_events[0]))

    theme_news_items: List[Mapping[str, Any]] = []
    theme_news_rows: List[Dict[str, str]] = []
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}:
        evidence_keys = {
            (str(item.get("title", "")).strip(), str(item.get("source", "")).strip())
            for item in evidence_rows
            if str(item.get("title", "")).strip()
        }
        theme_expansion_terms = _theme_news_expansion_terms(metadata, profile)
        theme_terms = _unique_strings(
            [
                sector,
                str(profile.get("profile_name", "")).strip(),
                *[str(item).strip() for item in profile.get("themes", []) if str(item).strip()],
                *[str(item).strip() for item in catalyst_keys if str(item).strip()],
                *[str(item).strip() for item in keyword_keys if str(item).strip()],
                *[str(item).strip() for item in _catalyst_search_terms(metadata, profile) if str(item).strip()],
                *theme_expansion_terms,
            ]
        )
        theme_scope_pool = _dedupe_news_items([*all_news_pool, *dynamic_related_news])
        theme_candidates = [
            item
            for item in theme_scope_pool
            if (str(item.get("title", "")).strip(), str(item.get("source", "")).strip()) not in evidence_keys
            and (
                _category_item_is_relevant(item, metadata, profile, allowed_categories, related_tokens, strict_tokens)
                or _contains_any(_title_source_text(item), theme_terms)
            )
        ]
        theme_news_items = _pick_top_news_items(
            theme_candidates,
            primary_keywords=theme_terms,
            bonus_keywords=[sector, *[str(item) for item in profile.get("themes", [])]],
            limit=2,
            reference_time=reference_now,
        )
        theme_news_rows = [
            _evidence_row(layer="主题级关键新闻", item=item)
            for item in theme_news_items
        ]

    directional_catalyst_available = asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and directional_award > 0
    direct_intelligence_available = bool(company_positive_pool or effective_structured_event or forward_events or directional_catalyst_available)
    stale_live_only = (
        news_mode == "live"
        and bool(all_news_pool or theme_news_rows)
        and not fresh_news_pool
        and not direct_intelligence_available
    )
    theme_only_live = (
        news_mode == "live"
        and bool(theme_news_rows)
        and not direct_intelligence_available
        and not company_positive_pool
    )
    theme_background_items = [
        item
        for item in theme_news_items
        if not (
            any(token in _title_source_text(item) for token in ("etf", "基金", "联接"))
            and any(
                token in _title_source_text(item)
                for token in ("净申购", "净流入", "净流出", "成交额", "换手率", "份额", "持有浮盈", "合计持有", "基金持有")
            )
        )
    ]
    theme_background_award = 0
    if asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and theme_only_live and theme_background_items:
        theme_background_sources = len(
            {
                str(item.get("source") or item.get("configured_source") or "").strip()
                for item in theme_background_items
                if str(item.get("source") or item.get("configured_source") or "").strip()
            }
        )
        theme_background_award = 6 if len(theme_background_items) >= 2 and theme_background_sources >= 2 else 4
        raw += theme_background_award
        available += 6
        factors.append(
            _factor_row(
                "主题级背景催化",
                f"live 主题情报 {len(theme_background_items)} 条",
                theme_background_award,
                6,
                "主题级 live 情报可以提供背景支持，但还没有穿透到 ETF/指数/核心成分层，不把它直接当成动作触发。",
                factor_id="j5_theme_background_support",
            )
        )
    search_gap_suspected = (
        asset_type_str in {"cn_stock", "cn_etf", "cn_fund"}
        and
        news_mode == "live"
        and dynamic_search_attempted
        and not dynamic_related_news
        and not effective_structured_event
        and not forward_events
        and not company_positive_pool
        and _expected_high_newsflow(metadata, profile)
    )
    score = None if search_gap_suspected else _normalize_dimension(raw, available, 100)
    catalyst_coverage = {
        "news_mode": news_mode,
        "high_confidence_company_news": bool(company_positive_pool),
        "structured_event": bool(structured_event_pool),
        "effective_structured_event": effective_structured_event,
        "forward_event": bool(forward_events),
        "directional_catalyst_hit": directional_award > 0 if asset_type_str in {"cn_etf", "cn_fund"} else False,
        "theme_news_count": len(theme_news_rows),
        "news_pool_count": len(all_news_pool),
        "fresh_news_pool_count": len(fresh_news_pool),
        "stale_news_pool_count": len(stale_news_pool),
        "recent_theme_news_pool_count": len(recent_theme_news_pool),
        "direct_news_count": (
            len(density_pool)
            if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}
            else len(company_positive_pool)
            if (
                (asset_type_str in {"hk", "us"} and stock_name_tokens)
                or (asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and instrument_identity_tokens)
            )
            else len(stock_specific_pool if stock_specific_pool else fresh_news_pool)
        ),
        "fresh_direct_news_count": (
            len(fresh_directional_pool)
            if asset_type_str in {"cn_etf", "cn_fund", "cn_index"}
            else len(company_positive_pool)
            if (
                (asset_type_str in {"hk", "us"} and stock_name_tokens)
                or (asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and instrument_identity_tokens)
            )
            else len(stock_specific_pool if stock_specific_pool else fresh_news_pool)
        ),
        "source_count": source_count,
        "latest_news_at": _latest_intelligence_at(all_news_pool, reference_now),
        "degraded": news_mode != "live",
        "search_attempted": dynamic_search_attempted,
        "search_groups": attempted_search_groups,
        "search_result_count": len(dynamic_related_news),
        "theme_background_support": theme_background_award > 0,
        "diagnosis": (
            "suspected_search_gap"
            if search_gap_suspected
            else "stale_live_only"
            if stale_live_only
            else "theme_only_live"
            if theme_only_live
            else "proxy_degraded"
            if news_mode != "live"
            else "confirmed_live"
        ),
        "ai_web_search_recommended": search_gap_suspected,
    }
    if search_gap_suspected:
        summary = "当前实时情报检索未命中高置信新增证据；对这类高关注方向更像搜索覆盖不足，本次催化维度暂按待 AI 联网复核处理，不直接记成零催化。"
    elif stale_live_only:
        summary = "当前能命中的多是旧闻回放或背景线索，新增催化仍不足；本次不把旧闻直接记成新催化。"
    elif theme_only_live:
        summary = (
            "当前主要是主题级情报，能提供轻度背景支持，但尚未命中公司/产品级直接催化；先按背景支持而不是动作触发处理。"
            if theme_background_award > 0
            else "当前主要是主题级情报，尚未命中公司/产品级直接催化；先按背景支持而不是动作触发处理。"
        )
    elif directional_catalyst_available and news_mode == "live":
        if asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and score is not None and score < 40:
            summary = "已命中 ETF/指数暴露方向的主题级 live 情报，说明背景催化不是空白；但还缺直接、强、可执行的新增催化，先按背景支持处理。"
        else:
            summary = "当前已经命中 ETF/指数暴露方向的 live 催化，说明主线情报开始穿透到可执行载体，但仍要看价格和量能能否确认。"
    elif asset_type_str in {"cn_etf", "cn_fund", "cn_index"} and not fresh_news_pool and recent_theme_news_pool:
        summary = "近 7 日主题/跟踪方向情报仍在延续，但最新 3 日没有新增高置信触发；本次按延续催化而不是 same-day 新催化处理。"
    elif score is None:
        summary = "ℹ️ 催化面数据缺失，本次评级未纳入该维度"
    elif effective_structured_event and score < 40:
        summary = "结构化事件已出现，但高质量公司级新闻确认还不够，当前更像事件在前、市场共识在后。"
    elif is_individual_stock and not effective_structured_event and not company_positive_pool and score < 40:
        summary = "当前未抓到高质量公司级新闻或结构化事件，先按信息不足处理，不直接视为利空。"
    else:
        summary = _dimension_summary(score, "催化明确，市场有理由重新定价。", "有催化苗头，但强度还不够形成一致预期。", "催化不足，当前更像静态博弈。", "ℹ️ 催化面数据缺失，本次评级未纳入该维度")
    return {
        "name": "催化面",
        "score": score,
        "max_score": 100,
        "summary": summary,
        "factors": factors,
        "core_signal": _catalyst_core_signal(
            factors,
            stock_specific_pool,
            company_positive_pool,
            is_individual_stock,
            asset_type_str,
            catalyst_coverage,
        ),
        "missing": score is None,
        "profile_name": profile.get("profile_name", sector),
        "coverage": catalyst_coverage,
        "evidence": _dedupe_news_items(evidence_rows),
        "theme_news": _dedupe_news_items(theme_news_rows),
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
        left_std = float(aligned.iloc[:, 0].std(ddof=0) or 0.0)
        right_std = float(aligned.iloc[:, 1].std(ddof=0) or 0.0)
        if np.isclose(left_std, 0.0) or np.isclose(right_std, 0.0):
            continue
        corr = float(aligned.iloc[:, 0].corr(aligned.iloc[:, 1]))
        if pd.isna(corr):
            continue
        if best_corr is None or abs(corr) > abs(best_corr):
            best_symbol = peer_symbol
            best_corr = corr
    if best_corr is None:
        return None
    return best_symbol, best_corr


def _sector_board_match(metadata: Mapping[str, Any], drivers: Mapping[str, Any]) -> Optional[float]:
    row, frame, _level = _matched_sector_spot_row(metadata, drivers)
    if row is None or frame is None or frame.empty:
        return None
    move_value = _row_number(row, ("涨跌幅", "今日涨跌幅"))
    if move_value is None:
        return None
    return float(move_value) / 100


def _sector_breadth_detail(metadata: Mapping[str, Any], drivers: Mapping[str, Any]) -> Dict[str, Any]:
    """J-3: Extract sector breadth details from industry_spot data.

    Returns:
        dict with keys:
        - advance_ratio: float, fraction of stocks advancing in sector (0-1), or None
        - sector_move: float, sector-level price change, or None
        - leader_up: bool, whether sector leader stocks are advancing
        - proxy_level: str, "sector_proxy" always (not individual stock level)
        - note: str, disclosure note
    """
    sector = str(metadata.get("sector", ""))
    row, frame, proxy_level = _matched_sector_breadth_row(metadata, drivers)
    if row is None or frame is None or frame.empty:
        row, frame, proxy_level = _matched_sector_spot_row(metadata, drivers)
    result: Dict[str, Any] = {
        "advance_ratio": None,
        "sector_move": None,
        "leader_up": None,
        "proxy_level": proxy_level or "sector_proxy",
        "note": "行业宽度数据缺失",
    }
    if row is None or frame is None or frame.empty:
        return result

    name_col = _first_column(frame, ("板块名称", "名称", "概念名称"))
    if name_col is None:
        return result
    matched_name = str(row.get(name_col, sector)).strip() or sector
    leader_col = _first_column(frame, ("领涨股票", "领涨股", "领涨证券"))
    leader_pct = _row_number(row, ("领涨涨跌幅", "领涨股票涨跌幅"))
    leader_name = str(row.get(leader_col, "")).strip() if leader_col else ""

    # Try to get advance/decline ratio from industry_spot
    # Some data sources provide 上涨家数/下跌家数 columns
    advance_col = next((c for c in frame.columns if "上涨" in c and "家数" in c), None)
    decline_col = next((c for c in frame.columns if "下跌" in c and "家数" in c), None)
    move_value = _row_number(row, ("涨跌幅", "今日涨跌幅"))
    if move_value is not None:
        result["sector_move"] = float(move_value) / 100

    if advance_col and decline_col:
        adv = pd.to_numeric(pd.Series([row.get(advance_col)]), errors="coerce").dropna()
        dec = pd.to_numeric(pd.Series([row.get(decline_col)]), errors="coerce").dropna()
        if not adv.empty and not dec.empty:
            total = float(adv.iloc[0]) + float(dec.iloc[0])
            if total > 0:
                result["advance_ratio"] = float(adv.iloc[0]) / total
                result["note"] = f"{matched_name} 上涨家数 {int(adv.iloc[0])}/{int(total)}，扩散比例 {result['advance_ratio']:.0%}"
    elif result["sector_move"] is not None:
        # Fallback: use sector move as proxy for breadth direction
        result["note"] = f"{matched_name} 涨跌幅 {result['sector_move']:.1%}（上涨家数缺失，用板块涨跌幅代理宽度方向）"

    # Leader confirmation: check if sector leaders are in the advancing group.
    # Hard constraint: leader_up can ONLY be set when advance_ratio is available
    # (i.e. actual advance/decline counts exist). Inferring leader_up from sector_move
    # would create a circular dependency — the same 涨跌幅 column would then score
    # 板块扩散 + 行业宽度 + 龙头确认 from a single data point.
    sector_chain = _derived_catalyst_profile(metadata)
    leaders = list(sector_chain.get("domestic_leaders", []))[:3]
    current_name = str(metadata.get("name", "")).strip()
    if leaders and result["advance_ratio"] is not None:
        if leader_pct is not None:
            result["leader_up"] = float(leader_pct) > 0
            leader_label = leader_name or "/".join(leaders[:2]) or "板块龙头"
            result["note"] += f"；领涨股 {leader_label} {float(leader_pct):+.2f}%"
        else:
            # Only infer leader direction when we have actual breadth data to anchor it.
            result["leader_up"] = result["advance_ratio"] >= 0.5
            result["note"] += f"；龙头代理（{'/'.join(leaders[:2])}）方向与板块一致" if result["leader_up"] else "；龙头代理方向与板块不一致，需关注分化"
    elif leaders and current_name and any(current_name in str(item) for item in leaders) and result["sector_move"] is not None:
        result["leader_up"] = float(result["sector_move"]) > 0
        result["note"] += f"；当前标的命中主题龙头列表（{'/'.join(leaders[:2])}），在上涨家数缺失时用板块方向做轻度代理"

    return result


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
    relative_cross_check_failed = False
    relative_score_cap: Optional[int] = None
    benchmark_spec = BENCHMARKS.get(asset_type)
    benchmark_symbol = str(benchmark_spec[0]).strip() if benchmark_spec else ""
    benchmark_name = str(benchmark_spec[2]).strip() if benchmark_spec else "基准"
    benchmark_returns = context.get("benchmark_returns", {}).get(asset_type)
    rel_5d = None
    rel_20d = None
    turnaround_only = False
    if benchmark_returns is not None and not benchmark_returns.empty:
        bench_5d = float(benchmark_returns.tail(5).sum())
        bench_20d = float(benchmark_returns.tail(20).sum())
        rel_5d = float(metrics.get("return_5d", 0.0)) - bench_5d
        rel_20d = float(metrics.get("return_20d", 0.0)) - bench_20d
        turnaround_only = rel_5d > 0 and rel_20d <= 0
        # Turnaround (20d negative → 5d positive) is most valuable; persistent excess is good too.
        # Bonus: large 5d excess (>5%) adds 5pts regardless of 20d direction.
        if turnaround_only:
            turn_award = 15 if rel_5d <= 0.05 else 18
        elif rel_20d > 0 and rel_5d > 0.05:
            turn_award = 25  # strong persistent outperformance
        elif rel_20d > 0:
            turn_award = 20
        elif rel_20d < -0.05 and rel_5d < -0.02:
            turn_award = -20
        elif rel_20d < 0 and rel_5d < 0:
            turn_award = -10
        else:
            turn_award = 0
        raw += turn_award
        available += 30
        factors.append(
            _factor_row(
                "超额拐点",
                f"相对基准（{benchmark_name}） 5日 {format_pct(rel_5d)} / 20日 {format_pct(rel_20d)}",
                turn_award,
                30,
                f"当前相对强弱基准使用 `{benchmark_name}`；相对基准从负转正更接近轮动切换窗口。",
                factor_id="j3_benchmark_relative",
            )
        )
    else:
        factors.append(
            _factor_row(
                "超额拐点",
                f"缺失（相对基准：{benchmark_name}）",
                None,
                30,
                f"基准收益序列缺失，未计算相对 `{benchmark_name}` 的超额拐点。",
                factor_id="j3_benchmark_relative",
            )
        )

    if asset_type in {"cn_etf", "cn_fund", "cn_index"}:
        peer_snapshot = _fund_recent_achievement_snapshot(context)
        history_days = int(len(asset_returns)) if asset_returns is not None else 0
        peer_percentile = peer_snapshot.get("percentile")
        peer_return_pct = peer_snapshot.get("return_pct")
        peer_rank_text = str(peer_snapshot.get("peer_rank_text", "")).strip()
        peer_period_label = str(peer_snapshot.get("period_label", "")).strip() or "近3月"
        cross_check_award = 0
        cross_check_signal = ""
        cross_check_detail = ""
        if turnaround_only or peer_rank_text:
            if turnaround_only:
                cross_check_signal = "短线修复已出现，但仍需同类业绩和样本长度校验"
                cross_check_detail = "5 日相对收益转强而 20 日相对收益仍未转正时，更像早期修复，不应只靠短周期超额就把它写成轮动已经确认。"
            if peer_percentile is not None and peer_return_pct is not None and peer_return_pct < 0:
                if peer_percentile >= 0.90:
                    cross_check_award -= 15
                    relative_cross_check_failed = True
                elif peer_percentile >= 0.80:
                    cross_check_award -= 10
                    relative_cross_check_failed = True
                cross_check_signal = (
                    f"{peer_period_label}同类排名 {peer_rank_text}"
                    + (f" / 区间收益 {peer_return_pct:+.2%}" if peer_return_pct is not None else "")
                )
                cross_check_detail = (
                    "同类排名仍落在后段且区间收益为负时，单看短周期相对收益很容易高估强度；"
                    "需要先把它理解成修复线索，而不是中期扩散已经完成。"
                )
            if turnaround_only and 60 <= history_days < 120:
                cross_check_award -= 5
                relative_cross_check_failed = True
                age_note = f"当前样本仅 {history_days} 个交易日。"
                cross_check_signal = cross_check_signal or f"样本仅 {history_days} 个交易日"
                cross_check_detail = (cross_check_detail + " " + age_note).strip()
            cross_check_award = max(min(cross_check_award, 6), -20)
            if relative_cross_check_failed:
                relative_score_cap = 65
            factors.append(
                _factor_row(
                    "同类业绩校验",
                    cross_check_signal or "当前未见需要额外上调/下调的同类业绩校验",
                    cross_check_award,
                    20,
                    cross_check_detail or "当前未见短线修复与中期业绩/样本长度之间的明显冲突。",
                )
            )
            raw += cross_check_award
            available += 20

    if asset_type in {"cn_etf", "cn_fund", "cn_index"} and _asset_uses_index_topic_bundle(metadata, fund_profile=context.get("fund_profile"), asset_type=asset_type):
        index_bundle = _context_index_topic_bundle(metadata, context, fund_profile=context.get("fund_profile"))
        history_snapshots = dict(index_bundle.get("history_snapshots") or {})
        for period, max_award in (("monthly", 6), ("weekly", 4)):
            history_snapshot = dict(history_snapshots.get(period) or {})
            if not history_snapshot or str(history_snapshot.get("status", "")).strip() != "matched":
                continue
            trend_label = str(history_snapshot.get("trend_label", "")).strip()
            momentum_label = str(history_snapshot.get("momentum_label", "")).strip()
            summary = str(history_snapshot.get("summary", "")).strip()
            period_label = {"weekly": "周线", "monthly": "月线"}.get(period, period)
            if trend_label in {"趋势偏强", "修复中"}:
                award = max_award
            elif trend_label == "趋势偏弱":
                award = -max_award
            else:
                award = 0
            if momentum_label == "动能偏强":
                award = min(award + 1, max_award)
            elif momentum_label == "动能偏弱":
                award = max(award - 1, -max_award)
            if trend_label or summary:
                raw += award
                available += max_award
                benchmark_label = str(
                    dict(index_bundle.get("index_snapshot") or {}).get("index_name")
                    or metadata.get("benchmark_name")
                    or metadata.get("benchmark")
                    or benchmark_name
                ).strip() or benchmark_name
                factors.append(
                    _factor_row(
                        f"指数{period_label}结构",
                        f"{benchmark_label} {trend_label}" + (f" / {momentum_label}" if momentum_label else ""),
                        award,
                        max_award,
                        (
                            f"标准指数主链已接入 `{benchmark_label}` 的 {period_label} 行情结构；"
                            + (f" {summary}" if summary else "")
                            + " 指数型产品的节奏先按更高频的结构确认。"
                        ).strip(),
                    )
                )
        technical_snapshot = dict(index_bundle.get("technical_snapshot") or {})
        trend_label = str(technical_snapshot.get("trend_label", "")).strip()
        momentum_label = str(technical_snapshot.get("momentum_label", "")).strip()
        pct_change = pd.to_numeric(pd.Series([technical_snapshot.get("pct_change")]), errors="coerce").iloc[0]
        if technical_snapshot and str(technical_snapshot.get("status", "")).strip() == "matched" and trend_label:
            benchmark_label = str(
                dict(index_bundle.get("index_snapshot") or {}).get("index_name")
                or metadata.get("benchmark_name")
                or metadata.get("benchmark")
                or benchmark_name
            ).strip() or benchmark_name
            if trend_label == "趋势偏强":
                index_award = 10
            elif trend_label == "修复中":
                index_award = 6
            elif trend_label == "趋势偏弱":
                index_award = -6
            else:
                index_award = 0
            if momentum_label == "动能偏强":
                index_award = min(index_award + 2, 10)
            elif momentum_label == "动能偏弱":
                index_award = max(index_award - 2, -8)
            raw += index_award
            available += 10
            factors.append(
                _factor_row(
                    "跟踪指数技术状态",
                    f"{benchmark_label} {trend_label}" + (f" / {momentum_label}" if momentum_label else ""),
                    index_award,
                    10,
                    (
                        f"标准指数主链已接入 `{benchmark_label}` 的 idx_factor_pro 技术状态；"
                        + (f" 当日涨跌幅 {float(pct_change):+.2f}% ." if not pd.isna(pct_change) else "")
                        + " ETF/指数型产品的相对强弱先按跟踪指数确认。"
                    ).strip(),
                )
            )
        else:
            factors.append(
                _factor_row(
                    "跟踪指数技术状态",
                    "缺失",
                    None,
                    10,
                    "当前未拿到可用 idx_factor_pro 跟踪指数状态，不把缺口误写成趋势已确认。",
                )
            )

    if asset_type in {"cn_stock", "hk", "hk_index"}:
        ah_snapshot = _cn_stock_ah_comparison_snapshot(metadata, context)
        ah_status = str(ah_snapshot.get("status", "")).strip()
        ah_is_fresh = bool(ah_snapshot.get("is_fresh"))
        ah_latest_date = str(ah_snapshot.get("latest_date", "")).strip()
        ah_premium_rate = pd.to_numeric(pd.Series([ah_snapshot.get("premium_rate")]), errors="coerce").iloc[0]
        ah_detail = str(ah_snapshot.get("detail", "")).strip()
        if ah_status == "matched" and ah_is_fresh and ah_premium_rate == ah_premium_rate:
            if asset_type == "cn_stock":
                if float(ah_premium_rate) <= 20:
                    ah_award = 10
                elif float(ah_premium_rate) <= 40:
                    ah_award = 6
                elif float(ah_premium_rate) <= 80:
                    ah_award = 2
                else:
                    ah_award = -4
                ah_signal = f"A/H 比价溢价 {float(ah_premium_rate):+.2f}%"
                ah_detail_text = "A 股相对 H 股溢价越高，跨市场估值压力越大；溢价收敛更像压力缓和。"
            else:
                if float(ah_premium_rate) >= 80:
                    ah_award = 10
                elif float(ah_premium_rate) >= 40:
                    ah_award = 6
                elif float(ah_premium_rate) >= 0:
                    ah_award = 2
                else:
                    ah_award = -4
                ah_signal = f"A/H 比价溢价 {float(ah_premium_rate):+.2f}%"
                ah_detail_text = "对港股相关标的，A/H 溢价越高，港股相对便宜，跨市场比价压力越缓和。"
            raw += ah_award
            available += 10
            factors.append(
                _factor_row(
                    "跨市场比价",
                    ah_signal + (f"（{ah_latest_date}）" if ah_latest_date else ""),
                    ah_award,
                    10,
                    f"{ah_detail_text} {ah_detail}" if ah_detail else ah_detail_text,
                    factor_id="j3_ah_comparison",
                    factor_meta_overrides={"source_as_of": ah_latest_date or None, "degraded": False},
                )
            )
        elif ah_status == "matched":
            factors.append(
                _factor_row(
                    "跨市场比价",
                    ah_detail or str(ah_snapshot.get("disclosure") or "A/H 比价快照非当期"),
                    0,
                    10,
                    str(ah_snapshot.get("disclosure") or "A/H 比价快照非当期，不把缺口误写成跨市场比价已确认。"),
                    display_score="观察",
                    factor_id="j3_ah_comparison",
                    factor_meta_overrides={
                        "source_as_of": ah_latest_date or None,
                        "degraded": True,
                        "degraded_reason": "Tushare stk_ah_comparison 非当期快照",
                    },
                )
            )
        else:
            factors.append(
                _factor_row(
                    "跨市场比价",
                    ah_detail or str(ah_snapshot.get("disclosure") or "A/H 比价数据缺失"),
                    None,
                    10,
                    str(ah_snapshot.get("disclosure") or "A/H 比价数据缺失，不把缺口误写成跨市场比价已确认。"),
                    display_score="观察",
                    factor_id="j3_ah_comparison",
                    factor_meta_overrides={
                        "degraded": True,
                        "degraded_reason": str(ah_snapshot.get("diagnosis", "missing")),
                    },
                )
            )

    domestic_sector_proxy = _uses_domestic_sector_proxy(asset_type, metadata, context)
    board_move = _sector_board_match(metadata, context.get("drivers", {})) if domestic_sector_proxy else None
    if board_move is not None:
        # Lowered threshold: 0.3% sector gain already qualifies as meaningful breadth.
        # Previous threshold of 1% was too strict for normal A-share sector rotations.
        breadth_award = 25 if board_move > 0.003 else 10 if board_move > 0 else -10 if board_move < -0.01 else -4 if board_move < 0 else 0
        raw += breadth_award
        available += 25
        factors.append(_factor_row("板块扩散", f"板块涨跌幅 {format_pct(board_move)}", breadth_award, 25, "板块内部越普涨，越像轮动扩散；这是行业级代理，不等于个股自身优势。"))
    elif not domestic_sector_proxy:
        factors.append(
            _factor_row(
                "板块扩散",
                "跨境/海外底层不直接使用 A 股板块涨跌幅",
                0,
                0,
                "当前产品主暴露属于跨境/海外底层，A 股行业板块涨跌幅容易形成错代理；这里保留观察提示，不把国内板块波动直接扣到产品相对强弱上。",
                display_score="观察提示",
            )
        )
    else:
        factors.append(_factor_row("板块扩散", "缺失", None, 25, "板块扩散数据缺失"))

    # J-3: 行业宽度细节（上涨家数/扩散比例）+ 龙头确认
    # 硬约束：行业级代理，不能写成个股自身优势
    breadth_detail = _sector_breadth_detail(metadata, context.get("drivers", {})) if domestic_sector_proxy else {}
    advance_ratio = breadth_detail.get("advance_ratio")
    leader_up = breadth_detail.get("leader_up")
    breadth_note = str(breadth_detail.get("note", "行业宽度数据缺失"))
    if advance_ratio is not None:
        # 上涨家数比例 > 60% 才算真正扩散
        industry_breadth_award = 15 if advance_ratio >= 0.60 else 8 if advance_ratio >= 0.45 else -8 if advance_ratio < 0.30 else -4 if advance_ratio < 0.40 else 0
        raw += industry_breadth_award
        available += 15
        factors.append(
            _factor_row(
                "行业宽度",
                f"行业上涨家数比例 {advance_ratio:.0%}",
                industry_breadth_award,
                15,
                f"行业宽度越高，说明轮动越扩散而不是只有龙头在涨；这是行业级信号，不等于个股自身优势。{breadth_note}",
                factor_id="j3_sector_breadth",
            )
        )
    else:
        # advance_ratio is None — no actual breadth data.
        # board_move (板块涨跌幅) is ALREADY captured in the 板块扩散 factor above.
        # Scoring 行业宽度 again from the same 涨跌幅 column would double-count the signal.
        # → Always observation_only when advance_ratio is absent.
        if not domestic_sector_proxy:
            factors.append(
                _factor_row(
                    "行业宽度",
                    "跨境/海外底层不直接使用 A 股行业宽度",
                    0,
                    0,
                    "当前产品主暴露属于跨境/海外底层，A 股上涨家数/下跌家数不适合作为主代理；这里只保留观察提示，避免把错市场宽度写成扩散确认。",
                    display_score="观察提示",
                    factor_id="j3_sector_breadth",
                )
            )
        elif board_move is not None:
            factors.append(
                _factor_row(
                    "行业宽度",
                    f"上涨家数缺失（板块涨跌幅 {format_pct(board_move)}，不二次计分）",
                    0,
                    0,
                    f"上涨家数/下跌家数数据缺失，板块涨跌幅已在板块扩散中计分，此处不重复评分以避免同源重复加分；代理层级：行业级。{breadth_note}",
                    display_score="观察提示",
                    factor_id="j3_sector_breadth",
                )
            )
        else:
            factors.append(_factor_row("行业宽度", "缺失", None, 15, "行业宽度（行业级代理）数据缺失，无法评估扩散程度。", factor_id="j3_sector_breadth"))

    # J-3: 龙头确认（行业级代理）
    # 龙头先涨、二线跟随是最健康的扩散结构；龙头不涨只有二线涨更像情绪炒作
    if leader_up is not None:
        leader_award = 10 if leader_up and board_move is not None and board_move > 0 else -5 if (leader_up is False and board_move is not None and board_move > 0) else 0
        raw += leader_award
        available += 10
        leader_signal = "龙头方向与板块一致，扩散结构健康" if leader_up else "龙头方向与板块不一致，需关注分化风险"
        leader_detail = "龙头确认是行业扩散的重要信号：龙头先涨、二线跟随是最健康的结构；龙头不涨只有二线涨更像情绪炒作。当前用行业龙头代理，不是个股自身信号。"
        factors.append(_factor_row("龙头确认", leader_signal, leader_award, 10, leader_detail, factor_id="j3_leader_confirmation"))
    else:
        matched_sector_move = breadth_detail.get("sector_move")
        if not domestic_sector_proxy:
            factors.append(
                _factor_row(
                    "龙头确认",
                    "跨境/海外底层不直接使用 A 股龙头代理",
                    0,
                    0,
                    "当前产品主暴露属于跨境/海外底层，A 股主题龙头和行业龙头不适合作为主确认；这里只保留观察提示，避免错把国内龙头结构写成产品本身的扩散确认。",
                    display_score="观察提示",
                    factor_id="j3_leader_confirmation",
                )
            )
        elif matched_sector_move is not None:
            factors.append(
                _factor_row(
                    "龙头确认",
                    "上涨家数缺失，暂不判断龙头确认",
                    0,
                    0,
                    "当前已经匹配到行业/概念板块，但缺少上涨家数/下跌家数，不能独立判断龙头是否与板块同步；这里保留为观察提示，避免同源重复推断。",
                    display_score="观察提示",
                    factor_id="j3_leader_confirmation",
                )
            )
        else:
            factors.append(_factor_row("龙头确认", "缺失", None, 10, "龙头确认数据缺失，无法评估行业扩散结构。", factor_id="j3_leader_confirmation"))

    theme_alignment_level = _theme_alignment_level(metadata, dict(context.get("day_theme") or {}))
    chain_award = 20 if theme_alignment_level == "direct" and float(metrics.get("return_5d", 0.0)) < 0.08 else 0
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
    regime_award = 15 if regime_match or theme_alignment_level == "direct" else 0
    raw += regime_award
    available += 15
    factors.append(_factor_row("Regime 适配", "与当前 regime / 主线方向一致" if regime_award else "当前 regime 对它没有额外加分", regime_award, 15, "大环境顺风时，轮动更容易持续"))

    bak_strength = metadata.get("bak_strength")
    bak_attack = metadata.get("bak_attack")
    bak_activity = metadata.get("bak_activity")
    if asset_type == "cn_stock" and any(value is not None for value in (bak_strength, bak_attack, bak_activity)):
        strength_value = float(bak_strength or 0.0)
        attack_value = float(bak_attack or 0.0)
        activity_value = float(bak_activity or 0.0)
        if strength_value >= 2.0 or attack_value >= 2.0:
            bak_award = 5
        elif strength_value > 0 or attack_value > 0:
            bak_award = 2
        else:
            bak_award = 0
        raw += bak_award
        available += 5
        factors.append(
            _factor_row(
                "日度强弱代理",
                f"Tushare bak_daily 强弱 {strength_value:.2f} / 攻击 {attack_value:.2f}",
                bak_award,
                5,
                f"这是 A 股日度增强快照的轻量补充项，活跃度 {activity_value:.0f}；只做小权重辅助，不单独决定是否推荐。",
            )
        )

    if asset_type not in {"cn_stock", "hk", "hk_index"}:
        ah_award = 10 if float(context.get("global_proxy", {}).get("dxy_20d_change", 0.0)) <= 0 else 0
        raw += ah_award
        available += 10
        factors.append(_factor_row("跨市场比价", "美元环境偏顺风" if ah_award else "该项不适用或暂无明显优势", ah_award, 10, "非 A/H 资产当前仅保留轻量美元环境代理。"))

    score = _normalize_dimension(raw, available, 100)
    if score is not None and relative_score_cap is not None:
        score = min(score, relative_score_cap)
    proxy_only = board_move is not None and (advance_ratio is None or leader_up is None)
    if score is None:
        summary = "ℹ️ 相对强弱数据缺失，本次评级未纳入该维度"
    elif relative_cross_check_failed:
        summary = "短线相对强弱虽有修复，但同类业绩/样本长度校验仍不支持把它写成扩散确认。"
    elif proxy_only:
        if score >= 40:
            summary = "相对强弱有改善，但行业宽度/龙头确认仍缺失，先按低置信代理理解。"
        else:
            summary = "相对强弱偏弱，且行业宽度/龙头确认仍缺失，更适合当成背景观察。"
    else:
        summary = _dimension_summary(
            score,
            "轮动已经轮到它，具备主线扩散条件。",
            "相对强弱有改善，但还不是最典型的扩散点。",
            "轮动还没轮到它，更多是背景观察。",
            "ℹ️ 相对强弱数据缺失，本次评级未纳入该维度",
        )
    return {
        "name": "相对强弱",
        "score": score,
        "max_score": 100,
        "summary": summary,
        "factors": factors,
        "core_signal": _top_material_signals(factors),
        "missing": score is None,
        "proxy_only": proxy_only,
        "turnaround_only": turnaround_only,
        "cross_check_failed": relative_cross_check_failed,
        "score_cap": relative_score_cap,
        "benchmark_name": benchmark_name,
        "benchmark_symbol": benchmark_symbol,
    }


def _sector_proxy_signal_label(metadata: Mapping[str, Any], matched_name: Any) -> str:
    matched = str(matched_name or "").strip()
    sector = str(metadata.get("sector") or "").strip()
    theme = str(metadata.get("theme") or "").strip()
    focus = sector or theme or matched or "相关行业/概念"
    if matched and matched != focus and matched not in focus:
        return f"相关行业/概念代理：{focus}（当前命中 {matched}）"
    if matched:
        return f"相关行业/概念代理：{matched}"
    return f"相关行业/概念代理：{focus}"


def _chips_dimension(
    symbol: str,
    asset_type: str,
    metadata: Mapping[str, Any],
    context: Mapping[str, Any],
    config: Mapping[str, Any],
    history: Optional[pd.DataFrame] = None,
) -> Dict[str, Any]:
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    display_name = "筹码结构（辅助项）" if asset_type in {"cn_etf", "cn_index", "cn_fund"} else "筹码结构"
    fund_profile = dict(context.get("fund_profile") or {})
    commodity_like_fund = _is_commodity_like_fund(asset_type, metadata, fund_profile)
    fund_profile_mode = str(fund_profile.get("profile_mode") or "").strip().lower()
    is_light_etf_profile = asset_type == "cn_etf" and fund_profile_mode == "light"
    drivers = dict(context.get("drivers", {}))
    sector_flow = _sector_flow_snapshot(metadata, drivers)
    northbound = _northbound_sector_snapshot(metadata, drivers)
    hot_rank = _hot_rank_snapshot(metadata, drivers)
    concentration_proxy: Dict[str, Any] = {}
    if (
        asset_type in {"cn_etf", "cn_index", "cn_fund"}
        and _asset_uses_index_topic_bundle(metadata, fund_profile=fund_profile, asset_type=asset_type)
        and not commodity_like_fund
        and not is_light_etf_profile
    ):
        concentration_proxy = _context_cn_index_concentration_proxy(
            _valuation_keywords(metadata),
            prefetched_bundle=metadata.get("index_topic_bundle"),
            context=context,
            config=config,
        )
    chip_snapshot = _cn_stock_chip_snapshot(metadata, context, history) if asset_type == "cn_stock" else {}
    capital_flow_snapshot = _cn_stock_capital_flow_snapshot(metadata, context) if asset_type == "cn_stock" else {}
    margin_snapshot = _cn_stock_margin_snapshot(metadata, context) if asset_type == "cn_stock" else {}

    heat_rank = hot_rank.get("rank") if not commodity_like_fund else None
    if heat_rank is not None:
        crowding_award = 30 if float(heat_rank) > 50 else 15 if float(heat_rank) > 20 else -10 if float(heat_rank) <= 10 else -5
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
        main_flow = float(sector_flow.get("main_flow") or 0.0)
        crowding_award = 30 if main_flow > 0 else -6 if main_flow < 0 else 0
        raw += crowding_award
        available += 30
        sector_proxy_label = _sector_proxy_signal_label(metadata, sector_flow.get("name"))
        factors.append(
            _factor_row(
                "公募/热度代理",
                f"{sector_proxy_label} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                crowding_award,
                30,
                "热门榜缺失时，改用相关行业资金流做低置信代理，主要用来观察配置方向，不把它当成个股自身热度已经确认。",
            )
        )
    else:
        factors.append(_factor_row("公募/热度代理", "缺失", None, 30, "公募低配/热度代理暂缺"))

    if asset_type == "cn_stock":
        chip_status = str(chip_snapshot.get("status", "")).strip()
        chip_is_fresh = bool(chip_snapshot.get("is_fresh"))
        chip_trade_gap_days = pd.to_numeric(pd.Series([chip_snapshot.get("trade_gap_days")]), errors="coerce").dropna()
        chip_is_t1_direct = chip_status == "matched" and not chip_is_fresh and not chip_trade_gap_days.empty and int(chip_trade_gap_days.iloc[0]) <= 1
        if chip_status == "matched" and (chip_is_fresh or chip_is_t1_direct):
            winner_max = 20 if chip_is_fresh else 15
            cost_max = 15 if chip_is_fresh else 12
            overhang_max = 15 if chip_is_fresh else 12
            distribution_max = 10 if chip_is_fresh else 8
            signal_suffix = "" if chip_is_fresh else "（上一交易日 T+1 直连）"
            lag_detail = "" if chip_is_fresh else " 当前筹码快照来自上一交易日直连数据，能回答成本区和套牢盘，但不等同今天盘中的新增资金。"
            winner_rate_pct = pd.to_numeric(pd.Series([chip_snapshot.get("winner_rate_pct")]), errors="coerce").dropna()
            price_vs_avg_pct = pd.to_numeric(pd.Series([chip_snapshot.get("price_vs_weight_avg_pct")]), errors="coerce").dropna()
            above_price_pct = pd.to_numeric(pd.Series([chip_snapshot.get("above_price_pct")]), errors="coerce").dropna()
            near_price_pct = pd.to_numeric(pd.Series([chip_snapshot.get("near_price_pct")]), errors="coerce").dropna()
            peak_price = pd.to_numeric(pd.Series([chip_snapshot.get("peak_price")]), errors="coerce").dropna()
            peak_percent = pd.to_numeric(pd.Series([chip_snapshot.get("peak_percent")]), errors="coerce").dropna()
            avg_cost = pd.to_numeric(pd.Series([chip_snapshot.get("weight_avg")]), errors="coerce").dropna()

            if not winner_rate_pct.empty:
                winner_value = float(winner_rate_pct.iloc[0])
                winner_award = (
                    20 if winner_value >= 70 else 12 if winner_value >= 55 else -12 if winner_value < 35 else -6 if winner_value < 45 else 0
                ) if chip_is_fresh else (
                    15 if winner_value >= 70 else 9 if winner_value >= 55 else -9 if winner_value < 35 else -5 if winner_value < 45 else 0
                )
                raw += winner_award
                available += winner_max
                factors.append(
                    _factor_row(
                        "筹码胜率",
                        f"盈利筹码约 {winner_value:.1f}%{signal_suffix}",
                        winner_award,
                        winner_max,
                        "真实筹码胜率越高，说明更多存量筹码已经处在盈利区，更容易形成趋势承接。" + lag_detail,
                        factor_id="j3_chip_winner",
                    )
                )
            else:
                factors.append(_factor_row("筹码胜率", "缺失", None, winner_max, "真实筹码胜率当前缺失。", factor_id="j3_chip_winner"))

            if not price_vs_avg_pct.empty and not avg_cost.empty:
                cost_gap = float(price_vs_avg_pct.iloc[0])
                avg_cost_value = float(avg_cost.iloc[0])
                cost_award = (
                    15 if cost_gap >= 0.05 else 8 if cost_gap >= 0 else -12 if cost_gap <= -0.08 else -6 if cost_gap < 0 else 0
                ) if chip_is_fresh else (
                    12 if cost_gap >= 0.05 else 6 if cost_gap >= 0 else -10 if cost_gap <= -0.08 else -5 if cost_gap < 0 else 0
                )
                raw += cost_award
                available += cost_max
                factors.append(
                    _factor_row(
                        "平均成本位置",
                        f"现价相对加权平均成本 {cost_gap:+.1%}（均价约 {avg_cost_value:.2f} 元）{signal_suffix}",
                        cost_award,
                        cost_max,
                        "现价站在平均成本上方，通常意味着新增承接更容易延续；反之说明存量套牢盘还没完全消化。" + lag_detail,
                        factor_id="j3_chip_cost_basis",
                    )
                )
            else:
                factors.append(_factor_row("平均成本位置", "缺失", None, cost_max, "当前未拿到可稳定使用的平均成本位置。", factor_id="j3_chip_cost_basis"))

            if not above_price_pct.empty:
                above_value = float(above_price_pct.iloc[0])
                overhang_award = (
                    15 if above_value <= 25 else 5 if above_value <= 40 else -10 if above_value >= 60 else -5 if above_value >= 45 else 0
                ) if chip_is_fresh else (
                    12 if above_value <= 25 else 4 if above_value <= 40 else -8 if above_value >= 60 else -4 if above_value >= 45 else 0
                )
                raw += overhang_award
                available += overhang_max
                factors.append(
                    _factor_row(
                        "套牢盘压力",
                        f"现价上方筹码约 {above_value:.1f}%{signal_suffix}",
                        overhang_award,
                        overhang_max,
                        "上方筹码越多，反弹越容易先碰到解套卖压；这项直接回答当前位置是不是还要先磨筹码。" + lag_detail,
                        factor_id="j3_chip_overhang",
                    )
                )
            else:
                factors.append(_factor_row("套牢盘压力", "缺失", None, overhang_max, "当前未拿到可稳定使用的上方套牢盘占比。", factor_id="j3_chip_overhang"))

            if not peak_price.empty and not peak_percent.empty:
                peak_price_value = float(peak_price.iloc[0])
                peak_percent_value = float(peak_percent.iloc[0])
                near_value = float(near_price_pct.iloc[0]) if not near_price_pct.empty else 0.0
                distribution_award = (
                    10 if near_value >= 30 else 5 if near_value >= 20 else -6 if peak_percent_value >= 15 and not price_vs_avg_pct.empty and float(price_vs_avg_pct.iloc[0]) < 0 else 0
                ) if chip_is_fresh else (
                    8 if near_value >= 30 else 4 if near_value >= 20 else -5 if peak_percent_value >= 15 and not price_vs_avg_pct.empty and float(price_vs_avg_pct.iloc[0]) < 0 else 0
                )
                raw += distribution_award
                available += distribution_max
                factors.append(
                    _factor_row(
                        "筹码密集区",
                        f"主筹码密集区约 {peak_price_value:.2f} 元 / 单价位占比 {peak_percent_value:.1f}%{signal_suffix}",
                        distribution_award,
                        distribution_max,
                        "主筹码如果已经聚到现价附近，更像在做换手确认；如果密集区明显压在现价上方，短线更像先消化抛压。" + lag_detail,
                        factor_id="j3_chip_distribution",
                    )
                )
            else:
                factors.append(_factor_row("筹码密集区", "缺失", None, distribution_max, "当前未拿到可稳定使用的筹码密集区。", factor_id="j3_chip_distribution"))
        else:
            chip_display = "缺失" if chip_status in {"empty", "blocked"} else "观察"
            if chip_status == "matched":
                chip_signal = f"真实筹码分布非当期（最新 {chip_snapshot.get('latest_date') or '未知'}）"
            elif chip_status in {"empty", "blocked"}:
                chip_signal = "真实筹码分布缺失"
            else:
                chip_signal = "真实筹码分布未启用"
            chip_detail = str(chip_snapshot.get("detail", "")).strip() or str(chip_snapshot.get("disclosure", "")).strip() or "真实筹码分布当前缺失，本轮先不把代理信号写成资金确认。"
            factors.append(_factor_row("真实筹码分布", chip_signal, None, 20, chip_detail, display_score=chip_display, factor_id="j3_real_chip_snapshot"))

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
                insider_penalty = 6 if net_ratio >= 0.1 else 3
                raw -= insider_penalty
                available += 10
                factors.append(
                    _factor_row(
                        "高管增持",
                        str(holdertrade.get("item", {}).get("title", "近 90 日存在净减持")),
                        -insider_penalty,
                        10,
                        "近 90 日净减持说明管理层/重要股东态度偏谨慎，当前按轻度拖累处理，不单独等于趋势反转。",
                        display_score=f"-{insider_penalty}",
                    )
                )
        else:
            factors.append(_factor_row("高管增持", "近 90 日未命中明确高管/大股东增减持", 0, 10, "当前未识别到可明确归因的股东增减持信号。", display_score="信息项"))

        holder_concentration = _cn_holder_concentration_snapshot(metadata, context)
        if holder_concentration:
            total_ratio = float(holder_concentration.get("total_ratio") or 0.0)
            float_ratio = float(holder_concentration.get("float_ratio") or 0.0)
            if total_ratio >= 50 or float_ratio >= 25:
                concentration_award_stock = 10
            elif total_ratio >= 35 or float_ratio >= 15:
                concentration_award_stock = 5
            else:
                concentration_award_stock = 0
            raw += concentration_award_stock
            available += 10
            factors.append(
                _factor_row(
                    "股东集中度",
                    str(holder_concentration.get("title", "最新股东集中度已披露")),
                    concentration_award_stock,
                    10,
                    str(holder_concentration.get("detail", "当前只把股东集中度当成筹码稳定性辅助，不直接等同于增量资金。")),
                )
            )
        else:
            factors.append(_factor_row("股东集中度", "前十大股东/流通股东数据缺失", 0, 10, "当前未识别到可稳定使用的前十大股东结构。", display_score="信息项"))
    else:
        factors.append(_factor_row("高管增持", "ETF / 指数产品不适用", 0, 0, "该因子主要适用于个股，不纳入 ETF 评分。", display_score="不适用"))
        factors.append(_factor_row("股东集中度", "ETF / 基金 / 指数产品不适用", 0, 0, "股东集中度主要适用于单一个股。", display_score="不适用"))

    if asset_type == "cn_etf" and commodity_like_fund:
        factors.append(_factor_row("北向/南向", "商品/期货 ETF 不适用", 0, 0, "北向资金是股票市场口径，不用于商品/期货 ETF。", display_score="不适用"))
    elif asset_type in {"cn_etf", "cn_stock"}:
        if northbound:
            north_value = northbound.get("net_value")
            north_float = float(north_value or 0.0)
            north_award = 20 if north_value is not None and north_float > 0 else -8 if north_value is not None and north_float < 0 else 0
            raw += north_award
            available += 20
            northbound_proxy_label = _sector_proxy_signal_label(metadata, northbound.get("name"))
            factors.append(
                _factor_row(
                    "北向/南向",
                    f"{northbound_proxy_label} 北向增持估计 {_fmt_yi_number(north_value)}",
                    north_award,
                    20,
                    "这里使用相关行业/概念板块北向增持排行，只能提示大致配置方向，不等于单一个股出现了明确北向增持。",
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
                    if is_light_etf_profile:
                        factors.append(
                            _factor_row(
                                "北向/南向",
                                "行业北向缺失，当前不再回退全市场北向总量",
                                0,
                                0,
                                "ETF 轻量预筛阶段优先保留行业/指数主链，不再为了全市场北向总量额外触发慢链。",
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
                        factors.append(
                            _factor_row(
                                "北向/南向",
                                f"北向净买额约 {_fmt_yi_number(value)}",
                                0,
                                0,
                                "行业北向缺失，当前只披露全市场方向代理；它可以提示市场风险偏好，但不能直接给单一 ETF 的筹码结构加分。",
                                display_score="信息项",
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
            if not is_light_etf_profile:
                flow = ChinaMarketCollector(config).get_etf_fund_flow(symbol)
                series = pd.to_numeric(flow.get("净流入", pd.Series(dtype=float)), errors="coerce").dropna()
                if not series.empty:
                    tail_flow = float(series.tail(5).sum())
                    chips_award = 10 if tail_flow > 0 else -5 if tail_flow < 0 else 0
                    raw += chips_award
                    available += 10
                    flow_signal = "ETF 近 5 个样本净流入为正" if tail_flow > 0 else "ETF 近 5 个样本净流出" if tail_flow < 0 else "ETF 流入没有持续为正"
                    factors.append(_factor_row("机构资金承接", flow_signal, chips_award, 10, "用 ETF 资金流做筹码代理"))
        except Exception as exc:
            if sector_flow and not commodity_like_fund:
                main_flow = float(sector_flow.get("main_flow") or 0.0)
                chips_award = 10 if main_flow > 0 else -5 if main_flow < 0 else 0
                raw += chips_award
                available += 10
                sector_proxy_label = _sector_proxy_signal_label(metadata, sector_flow.get("name"))
                factors.append(
                    _factor_row(
                        "机构资金承接",
                        f"{sector_proxy_label} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                        chips_award,
                        10,
                        f"ETF 流数据缺失，改用行业资金流代理: {exc}",
                    )
                )
            else:
                factors.append(_factor_row("机构资金承接", "缺失", None, 10, f"ETF 资金流数据缺失: {exc}"))
        if chips_award is None and not is_light_etf_profile:
            if sector_flow and not commodity_like_fund:
                main_flow = float(sector_flow.get("main_flow") or 0.0)
                chips_award = 10 if main_flow > 0 else -5 if main_flow < 0 else 0
                raw += chips_award
                available += 10
                sector_proxy_label = _sector_proxy_signal_label(metadata, sector_flow.get("name"))
                factors.append(
                    _factor_row(
                        "机构资金承接",
                        f"{sector_proxy_label} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                        chips_award,
                        10,
                        "ETF 资金流主链当前为空表，先用行业资金流代理承接方向，避免静默丢掉资金流判断。",
                    )
                )
            else:
                factors.append(
                    _factor_row(
                        "机构资金承接",
                        "ETF 资金流主链当前为空表",
                        None,
                        10,
                        "这轮没有拿到可用的 ETF 资金流样本，先显式按缺失处理，不静默吞掉该模块。",
                    )
                )
        if chips_award is None and is_light_etf_profile:
            if sector_flow and not commodity_like_fund:
                main_flow = float(sector_flow.get("main_flow") or 0.0)
                chips_award = 10 if main_flow > 0 else -5 if main_flow < 0 else 0
                raw += chips_award
                available += 10
                sector_proxy_label = _sector_proxy_signal_label(metadata, sector_flow.get("name"))
                factors.append(
                    _factor_row(
                        "机构资金承接",
                        f"{sector_proxy_label} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                        chips_award,
                        10,
                        "ETF discovery 轻量模式当前跳过 ETF 资金流慢链，先用行业资金流代理承接方向。",
                    )
                )
            else:
                factors.append(
                    _factor_row(
                        "机构资金承接",
                        "ETF 轻量预筛阶段未拉取资金流慢链",
                        None,
                        10,
                        "本轮先按标准行业/指数框架与产品结构预筛；如进入正式候选，再补完整 ETF 资金流。",
                        display_score="观察提示",
                    )
                )
    elif asset_type == "cn_stock":
        flow_status = str(capital_flow_snapshot.get("status", "")).strip()
        flow_is_fresh = bool(capital_flow_snapshot.get("is_fresh"))
        direct_main_flow = pd.to_numeric(pd.Series([capital_flow_snapshot.get("direct_main_flow")]), errors="coerce").dropna()
        direct_5d_flow = pd.to_numeric(pd.Series([capital_flow_snapshot.get("direct_5d_main_flow")]), errors="coerce").dropna()
        direct_trade_gap_days = pd.to_numeric(pd.Series([capital_flow_snapshot.get("direct_trade_gap_days")]), errors="coerce").dropna()
        proxy_main_flow = pd.to_numeric(pd.Series([capital_flow_snapshot.get("board_main_flow")]), errors="coerce").dropna()
        if flow_is_fresh and flow_status == "matched" and not direct_main_flow.empty:
            direct_today = float(direct_main_flow.iloc[0])
            direct_window = float(direct_5d_flow.iloc[0]) if not direct_5d_flow.empty else direct_today
            chips_award_stock = 15 if direct_today > 0 and direct_window > 0 else 10 if direct_today > 0 else -8 if direct_today < 0 and direct_window < 0 else -5 if direct_today < 0 else 0
            raw += chips_award_stock
            available += 15
            factors.append(
                _factor_row(
                    "机构资金承接",
                    f"个股主力净{'流入' if direct_today >= 0 else '流出'} {_fmt_yi_number(direct_today)}"
                    + (f" / 近 5 日累计 {_fmt_yi_number(direct_window)}" if not direct_5d_flow.empty else ""),
                    chips_award_stock,
                    15,
                    str(capital_flow_snapshot.get("detail", "")).strip()
                    or ("当前已接到个股级 moneyflow，不再只靠行业代理判断资金承接。" if direct_today >= 0 else "当前个股级 moneyflow 仍显示净流出，不把题材热度误写成承接确认。"),
                )
            )
        elif not direct_main_flow.empty and not direct_trade_gap_days.empty and int(direct_trade_gap_days.iloc[0]) <= 1:
            direct_today = float(direct_main_flow.iloc[0])
            direct_window = float(direct_5d_flow.iloc[0]) if not direct_5d_flow.empty else direct_today
            chips_award_stock = 12 if direct_today > 0 and direct_window > 0 else 8 if direct_today > 0 else -6 if direct_today < 0 and direct_window < 0 else -3 if direct_today < 0 else 0
            raw += chips_award_stock
            available += 12
            latest_direct_date = str(capital_flow_snapshot.get("latest_date", "")).strip()
            factors.append(
                _factor_row(
                    "机构资金承接",
                    f"上一交易日个股主力净{'流入' if direct_today >= 0 else '流出'} {_fmt_yi_number(direct_today)}"
                    + (f" / 近 5 日累计 {_fmt_yi_number(direct_window)}" if not direct_5d_flow.empty else "")
                    + "（T+1 直连）",
                    chips_award_stock,
                    12,
                    str(capital_flow_snapshot.get("detail", "")).strip()
                    or (
                        f"个股级 moneyflow 最新停在 {latest_direct_date or '上一交易日'}，属于 T+1 直连确认；"
                        "比行业代理更硬，但仍不等同今天盘中的新增资金。"
                    ),
                )
            )
        elif flow_is_fresh and flow_status == "proxy" and not proxy_main_flow.empty:
            proxy_today = float(proxy_main_flow.iloc[0])
            chips_award_stock = 10 if proxy_today > 0 else -5 if proxy_today < 0 else 0
            raw += chips_award_stock
            available += 10
            proxy_board_name = str(capital_flow_snapshot.get("board_name", "")).strip() or str(sector_flow.get("name", "")).strip()
            factors.append(
                _factor_row(
                    "机构资金承接",
                    f"{_sector_proxy_signal_label(metadata, proxy_board_name)} 主力净{'流入' if proxy_today >= 0 else '流出'} {_fmt_yi_number(proxy_today)}",
                    chips_award_stock,
                    10,
                    str(capital_flow_snapshot.get("detail", "")).strip()
                    or "个股资金流当前未命中 fresh，先用行业/概念资金流做代理，不把它当成公司自身资金已经形成明确承接。",
                )
            )
        elif sector_flow:
            main_flow = float(sector_flow.get("main_flow") or 0.0)
            chips_award_stock = 10 if main_flow > 0 else -5 if main_flow < 0 else 0
            raw += chips_award_stock
            available += 10
            sector_proxy_label = _sector_proxy_signal_label(metadata, sector_flow.get("name"))
            factors.append(
                _factor_row(
                    "机构资金承接",
                    f"{sector_proxy_label} 主力净{'流入' if (sector_flow.get('main_flow') or 0) > 0 else '流出'} {_fmt_yi_number(sector_flow.get('main_flow'))}",
                    chips_award_stock,
                    10,
                    "个股资金流当前只用相关行业主力资金流做低置信代理，不把它当成公司自身资金已经形成明确承接。",
                )
            )
        else:
            factors.append(_factor_row("机构资金承接", "缺失", None, 10, "个股所属行业资金流数据缺失"))

        margin_level = str(margin_snapshot.get("crowding_level", "")).strip()
        if bool(margin_snapshot.get("is_fresh")) and margin_level:
            if margin_level == "high":
                margin_signal = "融资盘升温明显，短线拥挤度偏高"
            elif margin_level == "medium":
                margin_signal = "融资盘仍在升温，需防一致性交易"
            elif margin_level == "relieved":
                margin_signal = "融资盘近窗口回落，拥挤度有所释放"
            else:
                margin_signal = "融资盘暂未见明显拥挤式抬升"
            factors.append(
                _factor_row(
                    "两融拥挤度",
                    margin_signal,
                    None,
                    0,
                    str(margin_snapshot.get("detail", "")).strip() or "个股两融明细只作拥挤度观察，不把它直接写成确定性利多/利空。",
                    display_score="观察提示",
                    factor_id="j3_margin_crowding",
                )
            )
        else:
            factors.append(
                _factor_row(
                    "两融拥挤度",
                    "两融明细缺失，暂不判断融资盘拥挤",
                    None,
                    0,
                    str(margin_snapshot.get("detail", "")).strip() or "当前未拿到可稳定使用的个股两融明细，不把缺口误写成融资盘已经退潮。",
                    display_score="观察提示",
                    factor_id="j3_margin_crowding",
                )
            )
    else:
        factors.append(_factor_row("机构资金承接", "该项不适用", None, 10, "当前只对 A 股 ETF 接稳定资金流代理"))

    top_concentration = concentration_proxy.get("top_concentration") if not commodity_like_fund else None
    if top_concentration is not None:
        concentration_award = 15 if float(top_concentration) >= 35 else 8 if float(top_concentration) >= 25 else 0
        raw += concentration_award
        available += 15
        concentration_source = str(concentration_proxy.get("source", "")).strip()
        if concentration_source == "index_topic_bundle":
            concentration_detail = "直接复用已命中的指数成分权重主链估算前五大集中度，不重复走慢速指数财务代理聚合。"
        else:
            concentration_detail = f"用指数前五大成分股权重集中度代理共识程度；财务覆盖权重约 {concentration_proxy.get('coverage_weight', 0.0):.1f}%。"
        factors.append(
            _factor_row(
                "机构集中度代理",
                f"前五大成分股权重合计 {float(top_concentration):.1f}%",
                concentration_award,
                15,
                concentration_detail,
            )
        )
    else:
        if commodity_like_fund:
            factors.append(_factor_row("机构集中度代理", "商品/期货 ETF 不适用", 0, 0, "这类产品不按股票成分股集中度衡量筹码结构。", display_score="不适用"))
        elif asset_type == "cn_stock":
            factors.append(
                _factor_row(
                    "机构集中度代理",
                    "个股主链不适用",
                    0,
                    0,
                    "个股主链当前按板块/主题/行业行情理解筹码与扩散，不再用指数成分股集中度代理个股判断。",
                    display_score="不适用",
                )
            )
        else:
            factors.append(_factor_row("机构集中度代理", "缺失", None, 15, "成分股权重集中度暂未接入"))

    # J-3: 拥挤度/反身性风险（observation_only，不进入主评分加分，只作为风险提示）
    # 硬约束：拥挤度是市场级/行业级信号，不等于个股自身风险
    # 当热度排名极高（前 10%）时，反身性风险上升，作为风险提示
    if heat_rank is not None and not commodity_like_fund:
        heat_rank_val = float(heat_rank)
        if heat_rank_val > 90:
            crowding_risk_signal = f"热度排名 {int(heat_rank_val)}，处于极高拥挤区，反身性风险上升"
            crowding_risk_detail = "热度排名极高时，市场共识已经非常一致，反身性风险（一致预期反转）上升；这是市场级/行业级信号，不等于个股一定会下跌，但需要更高的安全边际。"
        elif heat_rank_val > 70:
            crowding_risk_signal = f"热度排名 {int(heat_rank_val)}，处于较高拥挤区，需关注反身性"
            crowding_risk_detail = "热度排名较高时，市场共识偏一致，需关注反身性风险；当前只作为观察提示，不进入主评分。"
        else:
            crowding_risk_signal = f"热度排名 {int(heat_rank_val)}，拥挤度适中"
            crowding_risk_detail = "当前热度排名适中，拥挤度风险不突出。"
        factors.append(_factor_row("拥挤度风险", crowding_risk_signal, None, 0, crowding_risk_detail, display_score="观察提示", factor_id="j3_crowding"))
    elif not commodity_like_fund:
        factors.append(_factor_row("拥挤度风险", "热度数据缺失，无法评估拥挤度", None, 0, "拥挤度/反身性风险需要热度排名数据，当前缺失。", display_score="观察提示", factor_id="j3_crowding"))

    score = _normalize_dimension(raw, available, 100)
    proxy_only_individual = asset_type in {"cn_stock", "hk", "us"} and available < 40
    if score is not None and proxy_only_individual:
        score = min(score, 55)
    summary = _dimension_summary(score, "聪明钱方向偏正面。", "筹码结构没有形成明确增量共识。", "聪明钱没有明显站在这一边。", "ℹ️ 筹码结构数据缺失，本次评级未纳入该维度")
    chip_winner_factor = next((factor for factor in factors if factor.get("name") == "筹码胜率"), {})
    chip_cost_factor = next((factor for factor in factors if factor.get("name") == "平均成本位置"), {})
    chip_pressure_factor = next((factor for factor in factors if factor.get("name") == "套牢盘压力"), {})
    chip_distribution_factor = next((factor for factor in factors if factor.get("name") == "筹码密集区"), {})
    has_real_chip_signals = any(
        str(factor.get("signal", "")).strip() and str(factor.get("display_score", "")).strip() != "缺失"
        for factor in (chip_winner_factor, chip_cost_factor, chip_pressure_factor, chip_distribution_factor)
        if factor
    )
    stock_like_proxy_signal = asset_type in {"cn_stock", "hk", "us"} and any(
        "相关行业/概念代理" in str(factor.get("signal", ""))
        for factor in factors
    )
    if asset_type == "cn_stock" and has_real_chip_signals and score is not None:
        if int(chip_pressure_factor.get("awarded", 0) or 0) < 0 or int(chip_cost_factor.get("awarded", 0) or 0) < 0:
            summary = "真实筹码分布偏谨慎：平均成本或上方套牢盘压力还没完全消化，当前更像先磨筹码。"
        elif int(chip_winner_factor.get("awarded", 0) or 0) > 0 and int(chip_cost_factor.get("awarded", 0) or 0) >= 0:
            summary = "真实筹码分布开始配合价格：胜率和平均成本位置都偏正面，不只是行业代理在支撑。"
        else:
            summary = "真实筹码分布仍在拉锯：现价与平均成本接近，筹码换手还没走到明确一边。"
    elif proxy_only_individual and score is not None:
        if score >= 60:
            summary = "筹码代理偏正面，但当前更多来自行业/市场级信号，不把它当成单一个股已经被资金充分确认。"
        elif score >= 40:
            summary = "筹码代理没有形成明确增量共识，当前更多是行业/市场级观察信号。"
        else:
            summary = "筹码代理暂未显示明确承接，且当前更多来自行业/市场级信号，不把它当成公司资金面已经显著转强。"
    elif stock_like_proxy_signal and score is not None and score < 60:
        summary = "筹码代理暂未显示明确承接，当前更多来自行业/市场级信号，不把它当成公司资金面已经显著转强。"
    if asset_type in {"cn_etf", "cn_index", "cn_fund"}:
        if available <= 0:
            summary = "当前 ETF/基金 的筹码代理缺口较大，本轮主排序未使用该维度，先只作辅助披露。"
        else:
            summary += " 对 ETF/基金 只作辅助判断，当前主排序不会因为这项缺失而机械拉低。"
    return {
        "name": "筹码结构",
        "display_name": display_name,
        "score": score,
        "max_score": 100,
        "summary": summary,
        "factors": factors,
        "core_signal": _top_material_signals(factors),
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

    if asset_type in {"cn_etf", "cn_fund", "cn_index"}:
        peer_snapshot = _fund_recent_achievement_snapshot(context)
        peer_percentile = peer_snapshot.get("percentile")
        peer_return_pct = peer_snapshot.get("return_pct")
        peer_rank_text = str(peer_snapshot.get("peer_rank_text", "")).strip()
        peer_period_label = str(peer_snapshot.get("period_label", "")).strip() or "近3月"
        if peer_rank_text:
            if peer_percentile is not None and peer_return_pct is not None and peer_return_pct < 0:
                if peer_percentile >= 0.90:
                    peer_award = -12
                elif peer_percentile >= 0.80:
                    peer_award = -8
                else:
                    peer_award = 0
            elif peer_percentile is not None and peer_return_pct is not None and peer_percentile <= 0.30 and peer_return_pct > 0:
                peer_award = 4
            else:
                peer_award = 0
            raw += peer_award
            available += 12
            factors.append(
                _factor_row(
                    "同类业绩风险",
                    f"{peer_period_label}同类排名 {peer_rank_text}"
                    + (f" / 区间收益 {peer_return_pct:+.2%}" if peer_return_pct is not None else ""),
                    peer_award,
                    12,
                    "同类排名长期落在后段时，说明这只 ETF/基金 还没有证明自己具备稳定的中期承接；"
                    "即使短线有修复，也应保留更高的安全边际。",
                )
            )

    if asset_type == "cn_stock":
        regulatory_snapshot = _cn_stock_regulatory_risk_snapshot(metadata, context)
        if regulatory_snapshot:
            status = str(regulatory_snapshot.get("status", "")).strip()
            detail = str(regulatory_snapshot.get("detail", "")).strip() or "交易所风险专题当前未提供有效结论。"
            if status == "❌":
                penalty = 20
                raw -= penalty
                available += 20
                factors.append(
                    _factor_row(
                        "交易所风险提示",
                        "当前仍处于 ST / *ST 高风险状态",
                        0,
                        penalty,
                        detail,
                        display_score=f"-{penalty}",
                    )
                )
            elif status == "⚠️":
                penalty = 12 if int(regulatory_snapshot.get("active_alert_count") or 0) > 0 and int(regulatory_snapshot.get("high_shock_count") or 0) > 0 else 8
                raw -= penalty
                available += 20
                factors.append(
                    _factor_row(
                        "交易所风险提示",
                        "近窗口存在异常波动或重点提示",
                        0,
                        penalty,
                        detail,
                        display_score=f"-{penalty}",
                    )
                )
            elif status == "✅":
                award = 10
                raw += award
                available += 20
                factors.append(
                    _factor_row(
                        "交易所风险提示",
                        "近窗口未命中交易所高风险专题",
                        award,
                        20,
                        detail,
                    )
                )
            else:
                factors.append(
                    _factor_row(
                        "交易所风险提示",
                        "缺失",
                        None,
                        20,
                        detail,
                    )
                )

        margin_snapshot = _cn_stock_margin_snapshot(metadata, context)
        margin_level = str(margin_snapshot.get("crowding_level", "")).strip()
        margin_detail = str(margin_snapshot.get("detail", "")).strip() or "个股两融明细当前未提供有效结论。"
        if bool(margin_snapshot.get("is_fresh")) and margin_level == "high":
            penalty = 12
            raw -= penalty
            available += 12
            factors.append(
                _factor_row(
                    "两融拥挤",
                    "融资盘升温明显，短线拥挤度偏高",
                    0,
                    penalty,
                    margin_detail,
                    display_score=f"-{penalty}",
                )
            )
        elif bool(margin_snapshot.get("is_fresh")) and margin_level == "medium":
            penalty = 8
            raw -= penalty
            available += 12
            factors.append(
                _factor_row(
                    "两融拥挤",
                    "融资盘仍在升温，需防一致性交易",
                    0,
                    penalty,
                    margin_detail,
                    display_score=f"-{penalty}",
                )
            )
        elif bool(margin_snapshot.get("is_fresh")) and margin_level == "relieved":
            award = 4
            raw += award
            available += 12
            factors.append(
                _factor_row(
                    "两融拥挤",
                    "融资盘近窗口回落，拥挤度有所释放",
                    award,
                    12,
                    margin_detail,
                )
            )
        else:
            factors.append(
                _factor_row(
                    "两融拥挤",
                    "缺失" if not margin_snapshot else "当前两融信号不构成明确拥挤结论",
                    None if not margin_snapshot else 0,
                    12,
                    margin_detail,
                    display_score="信息项" if margin_snapshot else "",
                )
            )

        broker_snapshot = _cn_stock_broker_recommend_snapshot(metadata, context)
        broker_status = str(broker_snapshot.get("status", "")).strip()
        broker_crowding = str(broker_snapshot.get("crowding_level", "")).strip()
        broker_count = int(broker_snapshot.get("latest_broker_count") or 0)
        broker_consecutive = int(broker_snapshot.get("consecutive_months") or 0)
        broker_detail = str(broker_snapshot.get("detail", "")).strip() or "券商月度金股专题当前未提供有效结论。"
        if bool(broker_snapshot.get("is_fresh")) and broker_status == "matched" and broker_crowding == "high":
            penalty = 8 if broker_count >= 6 or broker_consecutive >= 4 else 5
            raw -= penalty
            available += 8
            factors.append(
                _factor_row(
                    "卖方一致预期过热",
                    "券商月度金股覆盖偏密，需防一致预期过热",
                    0,
                    penalty,
                    broker_detail,
                    display_score=f"-{penalty}",
                )
            )
        elif bool(broker_snapshot.get("is_fresh")) and broker_status == "matched" and broker_crowding == "medium":
            penalty = 4
            raw -= penalty
            available += 8
            factors.append(
                _factor_row(
                    "卖方一致预期过热",
                    "券商月度金股覆盖升温，需防预期先行交易",
                    0,
                    penalty,
                    broker_detail,
                    display_score=f"-{penalty}",
                )
            )
        else:
            factors.append(
                _factor_row(
                    "卖方一致预期过热",
                    "缺失" if not broker_snapshot else "当前卖方共识不构成明确过热结论",
                    None if not broker_snapshot else 0,
                    8,
                    broker_detail,
                    display_score="信息项" if broker_snapshot else "",
                )
            )

        board_action_snapshot = _cn_stock_board_action_snapshot(metadata, context, history)
        board_detail = str(board_action_snapshot.get("detail", "")).strip() or "打板专题当前未提供有效结论。"
        negative_board = bool(board_action_snapshot.get("has_negative_signal"))
        limit_times = pd.to_numeric(pd.Series([board_action_snapshot.get("limit_times")]), errors="coerce").dropna()
        strong_pool_hit = bool(board_action_snapshot.get("in_strong_pool"))
        dt_pool_hit = bool(board_action_snapshot.get("in_dt_pool"))
        near_up_limit = pd.to_numeric(pd.Series([board_action_snapshot.get("up_limit_gap_pct")]), errors="coerce").dropna()
        if bool(board_action_snapshot.get("is_fresh")) and negative_board:
            penalty = 10 if dt_pool_hit else 6
            raw -= penalty
            available += 10
            factors.append(
                _factor_row(
                    "打板情绪风险",
                    "龙虎榜/跌停/竞价信号偏负面",
                    0,
                    penalty,
                    board_detail,
                    display_score=f"-{penalty}",
                )
            )
        elif bool(board_action_snapshot.get("is_fresh")) and (
            (strong_pool_hit and not limit_times.empty and float(limit_times.iloc[0]) >= 2)
            or (not near_up_limit.empty and float(near_up_limit.iloc[0]) <= 0.005)
        ):
            penalty = 5
            raw -= penalty
            available += 10
            factors.append(
                _factor_row(
                    "打板情绪风险",
                    "情绪交易升温，需防打板过热",
                    0,
                    penalty,
                    board_detail,
                    display_score=f"-{penalty}",
                )
            )
        else:
            factors.append(
                _factor_row(
                    "打板情绪风险",
                    "缺失" if not board_action_snapshot else "当前未见明确打板过热风险",
                    None if not board_action_snapshot else 0,
                    10,
                    board_detail,
                    display_score="信息项" if board_action_snapshot else "",
                )
            )

    if asset_type in {"cn_stock", "hk", "us"}:
        stock_name_tokens = _stock_name_tokens(metadata)
        disclosure_pool: List[Mapping[str, Any]] = []
        news_items = context.get("news_report", {}).get("all_items") or context.get("news_report", {}).get("items", [])
        if stock_name_tokens:
            disclosure_pool.extend([item for item in news_items if _is_disclosure_like_item(item, stock_name_tokens)])
        stock_news_items: List[Mapping[str, Any]] = []
        if asset_type == "cn_stock":
            stock_news_items = _context_stock_news(symbol, context)
            stock_disclosure_items = [
                item
                for item in stock_news_items
                if _is_disclosure_like_item(item, stock_name_tokens)
            ]
            disclosure_pool = _dedupe_news_items([*disclosure_pool, *stock_disclosure_items])
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
        "summary": _dimension_summary(score, "下行空间和组合风险都还可控。", "风险和估值都不算轻松，仍要留足安全边际。", "风险收益比不占优，需更严控节奏。", "ℹ️ 风险特征数据缺失，本次评级未纳入该维度"),
        "factors": factors,
        "core_signal": _top_positive_signals(factors),
        "missing": score is None,
    }


def _seasonality_dimension(metadata: Mapping[str, Any], history: pd.DataFrame, context: Mapping[str, Any]) -> Dict[str, Any]:
    """J-2: Seasonal / calendar / event window factors.

    Upgrade from rule-based hints to scoreable factors with explicit sample boundaries.
    Hard constraints:
    - Must disclose sample size for any win-rate based factor
    - Degrade to observation-only when sample < MIN_SEASONAL_SAMPLES
    - Calendar rules are proxies; cannot be written as "will work this time"
    - Policy event windows are observation_only until lag/visibility fixture is complete
    """
    MIN_SEASONAL_SAMPLES = 3  # minimum years of same-month data to score

    dated_history = history.copy()
    dated_history["date"] = pd.to_datetime(dated_history["date"], errors="coerce")
    dated_history = dated_history.dropna(subset=["date"]).set_index("date").sort_index()
    close = dated_history["close"].astype(float)
    monthly = close.resample("ME").last().pct_change().dropna()
    month = datetime.now().month
    sector = str(metadata.get("sector", "综合"))
    asset_type = str(metadata.get("asset_type", ""))
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0

    # --- 月度胜率（J-2: 标的自身历史月度胜率）---
    # 样本边界必须显式披露；样本不足时降级为观察提示
    if not monthly.empty:
        month_series = monthly[monthly.index.month == month]
        n_samples = len(month_series)
        if n_samples >= MIN_SEASONAL_SAMPLES:
            win_rate = float((month_series > 0).mean())
            month_award = 25 if win_rate > 0.65 else 8 if win_rate > 0.50 else -15 if win_rate < 0.35 else -8 if win_rate < 0.45 else 0
            raw += month_award
            available += 25
            sample_note = f"样本 {n_samples} 年（{int(month_series.index.year.min())}–{int(month_series.index.year.max())}），历史胜率不等于本次必然有效。"
            factors.append(_factor_row("月度胜率", f"同月胜率 {win_rate:.0%}（{n_samples} 年样本）", month_award, 25, sample_note, factor_id="j2_monthly_win_rate"))
        else:
            # 样本不足，降级为观察提示，不进入评分
            sample_note = f"同月历史样本仅 {n_samples} 年，不足 {MIN_SEASONAL_SAMPLES} 年，降级为观察提示，不进入主评分。"
            factors.append(_factor_row("月度胜率", f"样本不足（{n_samples} 年）", None, 25, sample_note, factor_id="j2_monthly_win_rate"))
    else:
        factors.append(_factor_row("月度胜率", "缺失", None, 25, "缺少月度历史，无法计算同月胜率。", factor_id="j2_monthly_win_rate"))

    # --- 行业旺季前置窗口（J-2: 规则化日历代理）---
    in_window = month in MONTHLY_SEASONAL_WINDOWS.get(sector, set())
    window_award = 20 if in_window else 0
    raw += window_award
    available += 20
    window_detail = (
        f"当前 {month} 月处于 `{sector}` 行业常见旺季前置窗口；这是规则化日历代理，不等于本次一定有效，需结合实际景气数据确认。"
        if in_window
        else f"当前 {month} 月不在 `{sector}` 行业常见旺季前置窗口；如果行业景气数据有超预期，可以覆盖这个规则。"
    )
    factors.append(_factor_row("旺季前置", "位于常见旺季窗口" if in_window else "当前不在典型旺季前置窗口", window_award, 20, window_detail, factor_id="j2_sector_season"))

    # --- 财报前后窗口（J-2: 按季度日历，覆盖所有行业）---
    # 财报窗口：1月（Q3报）、4月（年报/Q1报）、7月（半年报预告）、10月（Q3报）
    # 前置观察期：前一个月（12/3/6/9月）
    earnings_core_months = {1, 4, 7, 10}
    earnings_pre_months = {12, 3, 6, 9}
    earnings_window_labels = {
        1: "年报/业绩预告",
        4: "年报和一季报",
        7: "中报预告/业绩快报",
        10: "三季报",
    }
    if month in earnings_core_months:
        # 财报密集期：博弈窗口，正负双向
        # 对成长/科技类加分（业绩超预期概率更高），对高股息/防御类中性
        earnings_award = 15 if sector in {"科技", "医药", "消费", "军工"} else 8
        earnings_signal = f"当前更接近 `{earnings_window_labels.get(month, '财报')}` 披露窗口，{sector} 行业业绩预期博弈升温"
        earnings_detail = "这是披露窗口代理，重点提示业绩预期博弈可能升温；它只能说明波动窗口更近，不等于公司一定会超预期。"
    elif month in earnings_pre_months:
        earnings_award = 5
        earnings_signal = "处于财报前置观察期，业绩预期博弈开始升温"
        earnings_detail = "财报前一个月通常是预期博弈升温期，机构开始调整仓位；当前只做轻度加分，不等于业绩一定超预期。"
    else:
        earnings_award = 0
        earnings_signal = "当前不在典型财报博弈窗口"
        earnings_detail = "当前月份不在财报密集期或前置观察期，财报窗口因子中性。"
    raw += earnings_award
    available += 15
    factors.append(_factor_row("财报窗口", earnings_signal, earnings_award, 15, earnings_detail, factor_id="j2_earnings_window"))

    # --- 指数调样/半年末/年末窗口（J-2）---
    # 主流 A 股指数（沪深300、中证500、中证1000）通常在 6 月和 12 月调样
    # 调样前 1-2 个月（4-5月、10-11月）是预期博弈期
    if month in {6, 12}:
        rebalance_award = 15
        rebalance_signal = f"{month} 月：主流指数调样窗口，潜在纳入/剔除标的博弈"
        rebalance_detail = "沪深300/中证500/中证1000 通常在 6 月和 12 月调样；被纳入预期的标的可能提前被动买入，被剔除预期的标的可能提前被动卖出。当前只做规则化代理，不等于个股一定被纳入。"
    elif month in {5, 11}:
        rebalance_award = 10
        rebalance_signal = f"{month} 月：指数调样前置观察期"
        rebalance_detail = "调样前一个月是预期博弈升温期，机构开始布局潜在纳入标的；当前只做轻度加分。"
    elif month in {4, 10}:
        rebalance_award = 5
        rebalance_signal = f"{month} 月：指数调样早期观察期"
        rebalance_detail = "调样前两个月开始有早期布局，但博弈强度较低；当前只做轻度加分。"
    else:
        rebalance_award = 0
        rebalance_signal = "当前不在典型调样窗口"
        rebalance_detail = "当前月份不在指数调样窗口或前置观察期，该因子中性。"
    raw += rebalance_award
    available += 15
    factors.append(_factor_row("指数调整", rebalance_signal, rebalance_award, 15, rebalance_detail, factor_id="j2_index_rebalance"))

    # --- 节假日消费/出行窗口（J-2: 按行业）---
    # 春节前后（1-2月）：消费/出行/旅游
    # 五一/十一（4-5月、9-10月）：消费/出行
    # 暑期（7-8月）：旅游/娱乐/教育
    HOLIDAY_WINDOWS: Dict[str, set] = {
        "消费": {1, 2, 4, 5, 9, 10},
        "旅游": {1, 2, 4, 5, 7, 8, 9, 10},
        "餐饮": {1, 2, 4, 5, 9, 10},
        "零售": {1, 2, 4, 5, 9, 10, 11, 12},
        "航空": {1, 2, 4, 5, 7, 8, 9, 10},
        "酒店": {1, 2, 4, 5, 7, 8, 9, 10},
    }
    holiday_sectors = [s for s, months in HOLIDAY_WINDOWS.items() if sector.startswith(s) or s in sector]
    in_holiday_window = any(month in HOLIDAY_WINDOWS.get(s, set()) for s in holiday_sectors)
    # 消费/旅游类行业在节假日窗口加分
    if in_holiday_window:
        holiday_award = 10
        holiday_signal = f"{month} 月：{sector} 行业节假日消费/出行窗口"
        holiday_detail = "节假日前后是消费/出行类行业的季节性顺风期；当前只做规则化代理，需结合实际出行/消费数据确认。"
    elif sector in {"消费", "旅游", "餐饮", "零售", "航空", "酒店"}:
        holiday_award = 0
        holiday_signal = "当前不在节假日消费/出行窗口"
        holiday_detail = "当前月份不在该行业的典型节假日窗口，季节性因子中性。"
    else:
        holiday_award = 0
        holiday_signal = "节假日窗口：该行业不适用"
        holiday_detail = "节假日窗口主要适用于需求会被假期直接抬升的行业；当前主题不按这条规则加分。"
    raw += holiday_award
    available += 10
    factors.append(_factor_row("节假日窗口", holiday_signal, holiday_award, 10, holiday_detail, factor_id="j2_holiday_window"))

    # --- 商品/能源季节性窗口（J-2）---
    # 能源：冬季取暖需求（10-12月）、夏季用电高峰（6-8月）
    # 有色金属：春季开工（3-4月）、年末备货（11-12月）
    # 农产品：播种/收获季节
    COMMODITY_WINDOWS: Dict[str, set] = {
        "能源": {6, 7, 8, 10, 11, 12},
        "煤炭": {10, 11, 12, 1},
        "天然气": {10, 11, 12, 1, 2},
        "有色": {3, 4, 11, 12},
        "钢铁": {3, 4, 5, 9, 10},
        "化工": {3, 4, 9, 10},
        "农业": {3, 4, 9, 10},
    }
    commodity_sectors = [s for s, months in COMMODITY_WINDOWS.items() if sector.startswith(s) or s in sector]
    in_commodity_window = any(month in COMMODITY_WINDOWS.get(s, set()) for s in commodity_sectors)
    if in_commodity_window:
        commodity_award = 10
        commodity_signal = f"{month} 月：{sector} 商品/能源季节性需求窗口"
        commodity_detail = "商品/能源类行业存在明显季节性需求规律；当前只做规则化代理，需结合实际库存/价格数据确认。"
    elif commodity_sectors:
        commodity_award = 0
        commodity_signal = "当前不在商品/能源季节性需求窗口"
        commodity_detail = "当前月份不在该商品/能源行业的典型季节性需求窗口，该因子中性。"
    else:
        commodity_award = 0
        commodity_signal = "商品季节性：该行业不适用"
        commodity_detail = "商品/能源季节性窗口主要适用于能源/有色/钢铁/化工/农业等行业。"
    raw += commodity_award
    available += 10
    factors.append(_factor_row("商品季节性", commodity_signal, commodity_award, 10, commodity_detail, factor_id="j2_commodity_season"))

    # --- 政策事件窗（J-2: observation_only，不进入主评分）---
    # 政策会议（两会3月、中央经济工作会议12月）、医保谈判（11-12月）、产业展会
    # 硬约束：没有完成 lag/visibility fixture 前，只作为观察提示，不进入主评分
    POLICY_EVENT_WINDOWS: Dict[str, Any] = {
        "两会": {"months": {3}, "sectors": {"综合", "科技", "医药", "消费", "军工", "电网"}},
        "中央经济工作会议": {"months": {12}, "sectors": {"综合", "科技", "消费", "高股息"}},
        "医保谈判": {"months": {11, 12}, "sectors": {"医药"}},
        "产业展会": {"months": {3, 4, 9, 10}, "sectors": {"科技", "消费", "汽车"}},
    }
    policy_hints: List[str] = []
    for event_name, event_info in POLICY_EVENT_WINDOWS.items():
        if month in event_info["months"] and (not event_info["sectors"] or sector in event_info["sectors"] or "综合" in event_info["sectors"]):
            policy_hints.append(event_name)
    if policy_hints:
        policy_signal = f"观察提示：{' / '.join(policy_hints)} 窗口期"
        policy_detail = f"当前处于 {' / '.join(policy_hints)} 窗口期，可能对 {sector} 行业有政策催化；但政策事件窗因子尚未完成 lag/visibility fixture，当前只作为观察提示，不进入主评分。"
    else:
        policy_signal = "当前无明确政策事件窗口"
        policy_detail = "当前月份未识别到与该行业相关的典型政策事件窗口。政策事件窗因子尚未完成 lag/visibility fixture，不进入主评分。"
    # 政策事件窗：observation_only，不加分，只作为信息项
    factors.append(
        _factor_row(
            "政策事件窗",
            policy_signal,
            None,
            0,
            policy_detail,
            display_score="观察提示",
            factor_id="j2_policy_event",
            factor_meta_overrides={
                "degraded": True,
                "degraded_reason": "政策事件窗尚未完成 lag / visibility fixture，当前只保留观察提示，不进入主评分。",
            },
        )
    )

    # --- 分红窗口（J-2: 高股息行业专属）---
    dividend_sectors = {"高股息", "银行", "电力", "公用事业", "煤炭"}
    in_dividend_sector = sector in dividend_sectors or any(s in sector for s in dividend_sectors)
    if in_dividend_sector and month in {4, 5, 6}:
        dividend_award = 10
        dividend_signal = f"{month} 月：高股息/分红行业抢权窗口"
        dividend_detail = "高股息/分红类行业在 4-6 月通常有抢权行情；当前只做规则化代理，需结合实际分红公告确认。"
    elif in_dividend_sector:
        dividend_award = 0
        dividend_signal = "当前不在典型分红博弈窗口"
        dividend_detail = "当前月份不在高股息行业的典型抢权窗口，该因子中性。"
    else:
        dividend_award = 0
        dividend_signal = "分红窗口：该行业不适用"
        dividend_detail = "分红窗口主要适用于高股息/银行/电力/公用事业等行业。"
    raw += dividend_award
    available += 10
    factors.append(_factor_row("分红窗口", dividend_signal, dividend_award, 10, dividend_detail, factor_id="j2_dividend_window"))

    score = _normalize_dimension(raw, available, 100)
    return {
        "name": "季节/日历",
        "score": score,
        "max_score": 100,
        "summary": _dimension_summary(
            score,
            "当前时间窗口相对有利，多个季节性因子共振。",
            "时间窗口中性，没有明显顺风或逆风。",
            "时间窗口不占优，更多靠主线和技术本身。",
            "ℹ️ 季节/日历数据缺失，本次评级未纳入该维度",
        ),
        "factors": factors,
        "core_signal": _top_material_signals(factors),
        "missing": score is None,
    }


def _macro_dimension(metadata: Mapping[str, Any], context: Mapping[str, Any]) -> Dict[str, Any]:
    china_macro = dict(context.get("china_macro") or {})
    regime = dict(context.get("regime") or {})
    sector = str(metadata.get("sector", "综合"))
    vector = SENSITIVITY_MAP.get(str(metadata.get("sector", "综合")), {"rate": 0, "usd": 0, "oil": 0, "cny": 0})
    leading_profile = MACRO_LEADING_MAP.get(sector, {"recovery": 0, "credit": 0, "reflation": 0, "defensive": 0})
    states = _current_factor_state(context)
    match_count = 0
    active = 0
    details: List[str] = []
    factors: List[Dict[str, Any]] = []
    raw = 0
    available = 0
    for factor in ("rate", "usd", "oil", "cny"):
        direction = int(vector.get(factor, 0))
        if direction == 0:
            continue
        active += 1
        same = direction == states.get(factor)
        match_count += 1 if same else 0
        details.append(f"{factor} {'顺风' if same else '逆风'}")
    if active == 0:
        sensitivity_award = 20
    elif match_count >= 3:
        sensitivity_award = 40
    elif match_count == 2:
        sensitivity_award = 30
    elif match_count == 1:
        sensitivity_award = 10
    elif active >= 3:
        sensitivity_award = -10
    else:
        sensitivity_award = 0
    raw += sensitivity_award
    available += 40
    factors.append(
        _factor_row(
            "敏感度向量",
            " / ".join(details) if details else "当前定义为中性敏感度",
            sensitivity_award,
            40,
            "利率 / 美元 / 油价 / 人民币 四因子匹配。",
            factor_id="m1_sensitivity_vector",
        )
    )

    demand_state = str(china_macro.get("demand_state", "stable"))
    pmi = float(china_macro.get("pmi", 50.0))
    new_orders = float(china_macro.get("pmi_new_orders", pmi))
    production = float(china_macro.get("pmi_production", pmi))
    recovery_preference = int(leading_profile.get("recovery", 0))
    if demand_state == "improving":
        demand_award = 20 if recovery_preference >= 1 else 10 if recovery_preference == 0 else 0
        demand_signal = f"PMI {pmi:.1f} / 新订单 {new_orders:.1f} / 生产 {production:.1f}，景气领先指标改善"
    elif demand_state == "weakening":
        demand_award = 20 if recovery_preference <= -1 else -8 if recovery_preference >= 1 else 4
        demand_signal = f"PMI {pmi:.1f} / 新订单 {new_orders:.1f}，景气领先指标偏弱"
    else:
        demand_award = 12 if recovery_preference == 0 else 8
        demand_signal = f"PMI {pmi:.1f} / 新订单 {new_orders:.1f}，景气方向暂时中性"
    raw += demand_award
    available += 20
    factors.append(_factor_row("景气方向", demand_signal, demand_award, 20, "优先看 PMI、新订单和生产分项，而不是只看一个 PMI 总量。", factor_id="m1_demand_cycle"))

    ppi = float(china_macro.get("ppi_yoy", 0.0))
    price_state = str(china_macro.get("price_state", "stable"))
    reflation_preference = int(leading_profile.get("reflation", 0))
    if price_state == "reflation":
        price_award = 15 if reflation_preference >= 1 else 8 if reflation_preference == 0 else 0
        price_signal = f"PPI {ppi:.1f}% 且趋势回升，价格链条偏修复"
    elif price_state == "disinflation":
        price_award = 15 if reflation_preference <= -1 else -6 if reflation_preference >= 1 else 6
        price_signal = f"PPI {ppi:.1f}% 且趋势走弱，价格链条偏通缩"
    else:
        price_award = 8 if reflation_preference == 0 else 5
        price_signal = f"PPI {ppi:.1f}% ，价格链条暂时中性"
    raw += price_award
    available += 15
    factors.append(_factor_row("价格链条", price_signal, price_award, 15, "PPI 更适合判断未来 3-6 个月的上游价格与利润链条，不直接替代个股基本面。", factor_id="m1_price_chain"))

    credit_impulse = str(china_macro.get("credit_impulse", "stable"))
    spread = float(china_macro.get("m1_m2_spread", 0.0))
    sf_avg_text = str(china_macro.get("social_financing_3m_avg_text", "—"))
    credit_preference = int(leading_profile.get("credit", 0))
    if credit_impulse == "expanding":
        credit_award = 15 if credit_preference >= 1 else 8 if credit_preference == 0 else 0
        credit_signal = f"M1-M2 剪刀差 {spread:+.1f}pct，社融近3月均值 {sf_avg_text}，信用脉冲扩张"
    elif credit_impulse == "contracting":
        credit_award = 15 if credit_preference <= -1 else -6 if credit_preference >= 1 else 4
        credit_signal = f"M1-M2 剪刀差 {spread:+.1f}pct，信用脉冲收缩"
    else:
        credit_award = 8 if credit_preference == 0 else 5
        credit_signal = f"M1-M2 剪刀差 {spread:+.1f}pct，信用脉冲暂时中性"
    raw += credit_award
    available += 15
    factors.append(_factor_row("信用脉冲", credit_signal, credit_award, 15, "更偏中期环境因子，主要影响资金扩张、订单兑现和风险偏好。", factor_id="m1_credit_impulse"))

    regime_name = str(regime.get("current_regime", "unknown"))
    defensive_preference = int(leading_profile.get("defensive", 0))
    if regime_name in {"stagflation", "deflation"}:
        regime_award = 10 if defensive_preference >= 1 else 4 if defensive_preference == 0 else 0
        regime_signal = f"当前背景 `{regime_name}`，偏防守/避险"
    elif regime_name == "recovery":
        regime_award = 10 if recovery_preference >= 1 else -4 if recovery_preference <= -1 else 4
        regime_signal = "当前背景 `recovery`，偏景气修复"
    elif regime_name == "overheating":
        regime_award = 10 if reflation_preference >= 1 else -4 if reflation_preference <= -1 else 4
        regime_signal = "当前背景 `overheating`，更利于资源/通胀受益方向"
    else:
        regime_award = 4
        regime_signal = f"当前背景 `{regime_name}`，暂按中性处理"
    raw += regime_award
    available += 10
    factors.append(_factor_row("当前 regime", regime_signal, regime_award, 10, "把中期宏观环境映射到当前板块/标的，不单独决定方向。", factor_id="m1_regime_context"))

    score = _normalize_dimension(raw, available, 40)
    return {
        "name": "宏观敏感度",
        "score": score,
        "max_score": 40,
        "summary": _dimension_summary(
            score,
            "宏观领先指标和当前风格都偏顺风。",
            "宏观没有明显顺风，但也不是决定性逆风。",
            "宏观领先指标和当前风格都偏逆风，更适合保守处理。",
            "ℹ️ 宏观领先指标缺失，本次评级未纳入该维度",
            max_score=40,
        ),
        "factors": factors,
        "core_signal": _top_material_signals(factors),
        "missing": False,
        "macro_reverse": (score or 0) <= 5,
    }


def _stock_signal_consistency_gate(
    asset_type: str,
    dimensions: Mapping[str, Mapping[str, Any]],
) -> Dict[str, Any]:
    if asset_type != "cn_stock":
        return {"applies": False}

    def score(key: str) -> Optional[int]:
        value = dimensions.get(key, {}).get("score")
        return int(value) if value is not None else None

    checks = (
        ("技术面", score("technical"), 30),
        ("催化面", score("catalyst"), 20),
        ("风险特征", score("risk"), 20),
    )
    failed = [(label, item_score, threshold) for label, item_score, threshold in checks if item_score is not None and item_score < threshold]
    if not failed:
        return {"applies": False}

    target_rank = 0 if len(failed) >= 2 else 1
    failed_text = "、".join(f"{label}{item_score}/{threshold}" for label, item_score, threshold in failed)
    if target_rank == 0:
        warning = f"⚠️ 个股信号硬门槛未过（{failed_text}），结论直接压回无信号，不允许靠单一强因子包装成推荐。"
    else:
        warning = f"⚠️ 个股信号存在硬门槛短板（{failed_text}），结论封顶为观察级，不允许输出较强机会。"
    return {
        "applies": True,
        "target_rank": target_rank,
        "failed": failed,
        "warning": warning,
    }


def _rating_from_dimensions(
    dimensions: Mapping[str, Mapping[str, Any]],
    warnings: Sequence[str],
    *,
    asset_type: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    tech = dimensions["technical"]["score"]
    fundamental = dimensions["fundamental"]["score"]
    catalyst = dimensions["catalyst"]["score"]
    relative = dimensions["relative_strength"]["score"]
    risk = dimensions["risk"]["score"]
    macro = dimensions["macro"]["score"]
    relative_cross_check_failed = bool(dimensions.get("relative_strength", {}).get("cross_check_failed"))
    catalyst_coverage = dict(dimensions["catalyst"].get("coverage") or {})
    has_structured_or_direct_catalyst = bool(
        catalyst_coverage.get("structured_event")
        or catalyst_coverage.get("effective_structured_event")
        or catalyst_coverage.get("forward_event")
        or catalyst_coverage.get("high_confidence_company_news")
        or int(catalyst_coverage.get("direct_news_count") or 0) > 0
    )

    def ok(value: Optional[int], threshold: int) -> bool:
        return value is not None and value >= threshold

    def generic_rating_copy(target_rank: int) -> tuple[str, str]:
        if target_rank >= 4:
            return "强机会", "四维共振，具备建仓计划条件。"
        if target_rank == 3:
            return "较强机会", "逻辑成立，但执行上仍需按节奏确认。"
        if target_rank == 2:
            return "储备机会", "局部信号存在，但关键条件还没通过，先按观察/储备处理。"
        if target_rank == 1:
            return "有信号但不充分", "只有单一维度足够亮，其余不足以支持动作。"
        return "无信号", "没有形成可执行的多维共振。"

    def macro_resilient_rank_three() -> bool:
        return (
            (ok(tech, 45) and ok(relative, 70))
            or (ok(fundamental, 75) and ok(catalyst, 50) and ok(tech, 35))
            or (ok(fundamental, 80) and ok(catalyst, 15) and ok(relative, 30) and ok(tech, 20))
            or (asset_type == "cn_stock" and ok(fundamental, 70) and ok(catalyst, 20) and ok(relative, 30) and ok(tech, 15))
            or (
                asset_type in {"cn_etf", "cn_fund", "cn_index"}
                and (
                    (ok(fundamental, 50) and ok(risk, 55) and ok(relative, 35) and ok(tech, 25))
                    or (ok(fundamental, 40) and ok(catalyst, 25) and ok(relative, 25) and ok(tech, 35))
                    or (ok(fundamental, 25) and ok(catalyst, 18) and ok(relative, 50) and ok(tech, 35))
                )
            )
        )

    rank = 0
    label = "无信号"
    meaning = "没有形成可执行的多维共振。"
    if ok(tech, 70) and ok(fundamental, 60) and ok(catalyst, 50) and ok(risk, 50):
        rank, label, meaning = 4, "强机会", "四维共振，具备建仓计划条件。"
    elif ok(fundamental, 60) and (ok(catalyst, 50) or ok(relative, 60)) and ok(tech, 35):
        rank, label, meaning = 3, "较强机会", "逻辑成立，右侧执行仍需一个维度继续确认。"
    elif ok(tech, 55) and ok(relative, 65) and ok(risk, 45) and (ok(catalyst, 25) or ok(fundamental, 35)):
        rank, label, meaning = 3, "较强机会", "趋势和轮动已经形成共振，不必因为赔率还不完美就过度降级。"
    elif ok(fundamental, 60) and ok(catalyst, 25) and ok(relative, 25) and ok(tech, 20):
        rank, label, meaning = 3, "较强机会", "震荡市里逻辑、事件和相对位置已开始共振，不必等所有维度都满格才允许试仓。"
    elif ok(fundamental, 80) and ok(catalyst, 15) and ok(relative, 30) and ok(tech, 20):
        rank, label, meaning = 3, "较强机会", "高质量龙头在震荡市里已有相对强度和事件线索，不必等趋势彻底顺滑才允许进入标准推荐口径。"
    elif asset_type == "cn_stock" and ok(fundamental, 70) and ok(catalyst, 20) and ok(relative, 20) and ok(tech, 15):
        rank, label, meaning = 3, "较强机会", "震荡市里个股只要基本面够硬、催化不空、相对位置没坏，就不该一律压回观察稿。"
    elif asset_type == "cn_stock" and ok(fundamental, 65) and ok(relative, 25) and ok(tech, 15) and has_structured_or_direct_catalyst:
        rank, label, meaning = 3, "较强机会", "公司级结构化事件或直连情报已经在抬升确定性，允许先进入标准推荐口径再等价格确认。"
    elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and ok(fundamental, 50) and ok(risk, 55) and ok(relative, 35) and ok(tech, 25):
        rank, label, meaning = 3, "较强机会", "配置型产品已经具备防守收益比和相对强弱，不应被一刀切压成观察稿。"
    elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and ok(fundamental, 40) and ok(catalyst, 25) and ok(relative, 25) and ok(tech, 35):
        rank, label, meaning = 3, "较强机会", "主题型产品已有催化和价格承接，允许按右侧确认前的标准推荐口径处理。"
    elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and ok(fundamental, 40) and ok(catalyst, 20) and ok(relative, 45) and ok(tech, 35):
        rank, label, meaning = 3, "较强机会", "跨境主题产品已经有相对强弱和延续催化，不该因为缺 same-day 新催化就一律压回观察稿。"
    elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and ok(fundamental, 25) and ok(catalyst, 18) and ok(relative, 50) and ok(tech, 35):
        rank, label, meaning = 3, "较强机会", "主题 ETF 已有延续催化和相对强势，震荡市里不该因为没有公司级公告就一律压回观察稿。"
    elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and ok(fundamental, 60) and ok(relative, 55) and ok(tech, 30):
        rank, label, meaning = 3, "较强机会", "产品承接、相对强弱和技术位置已经站住，震荡市里不必强等单条催化标题才允许 ETF 进入标准推荐口径。"
    elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and ok(fundamental, 45) and ok(relative, 40) and ok(tech, 20) and (ok(catalyst, 15) or ok(risk, 45)):
        rank, label, meaning = 3, "较强机会", "震荡市里的 ETF 更应该看趋势承接、相对强弱和风险收益比，不必强等当天爆点新闻才允许正式推荐。"
    elif asset_type == "cn_stock" and ok(fundamental, 70) and ok(catalyst, 20) and ok(relative, 30) and ok(tech, 15):
        rank, label, meaning = 3, "较强机会", "震荡市里优质个股只要基本面、催化和相对位置都没坏，就不该被机械地压回观察稿。"
    elif (ok(tech, 70) and catalyst is not None and catalyst < 50) or (ok(catalyst, 60) and tech is not None and tech < 40):
        rank, label, meaning = 2, "储备机会", "单维度亮灯但还未形成共振。"
    else:
        strong_dims = sum(1 for item in dimensions.values() if item.get("score") is not None and item.get("score", 0) >= 70)
        mid_dims = sum(1 for item in dimensions.values() if item.get("score") is not None and item.get("score", 0) >= 40)
        if strong_dims == 1 and mid_dims <= 2:
            rank, label, meaning = 1, "有信号但不充分", "只有单一维度足够亮，其余不足以支持动作。"

    pre_cap_rank = rank
    if dimensions["macro"].get("macro_reverse"):
        if rank >= 4:
            rank = 3
            warnings = list(warnings) + ["⚠️ 宏观敏感度完全逆风，评级上限已压到 ⭐⭐⭐"]
        elif rank >= 3 and not macro_resilient_rank_three():
            rank = 2
            warnings = list(warnings) + ["⚠️ 宏观敏感度完全逆风，评级上限已压到 ⭐⭐"]
        elif rank >= 3:
            warnings = list(warnings) + ["⚠️ 宏观敏感度完全逆风，但当前走势/基本面韧性足以保留 ⭐⭐⭐ 观察上限"]
    if tech is None:
        rank = min(rank, 2)
        warnings = list(warnings) + ["ℹ️ 技术面数据缺失，评级上限降至 ⭐⭐"]
    if dimensions["fundamental"].get("available_max", 0) < 30:
        rank = min(rank, 3)
        warnings = list(warnings) + ["ℹ️ 基本面当前以代理因子为主，评级上限降至 ⭐⭐⭐"]
    if (
        asset_type in {"cn_etf", "cn_fund", "cn_index"}
        and rank >= 3
        and relative_cross_check_failed
        and ((catalyst is not None and catalyst < 20) or (tech is not None and tech < 50))
    ):
        rank = 2
        warnings = list(warnings) + ["⚠️ 短线相对强弱与同类业绩/样本长度不匹配，ETF 结论先压回观察/储备级别"]

    stock_signal_gate = _stock_signal_consistency_gate(asset_type, dimensions)
    if stock_signal_gate.get("applies"):
        target_rank = int(stock_signal_gate.get("target_rank") if stock_signal_gate.get("target_rank") is not None else 1)
        if rank > target_rank:
            rank = target_rank
        warnings = list(warnings) + [str(stock_signal_gate.get("warning") or "")]

    theme_confirmation_gate = _fund_like_theme_confirmation_gate(asset_type, metadata, dimensions)
    if theme_confirmation_gate.get("applies") and rank >= 2:
        rank = 1
        warnings = list(warnings) + [str(theme_confirmation_gate.get("warning") or "")]

    if rank != pre_cap_rank and rank <= 2:
        label, meaning = generic_rating_copy(rank)

    stars = "—" if rank == 0 else "⭐" * rank
    return {"rank": rank, "stars": stars, "label": label, "meaning": meaning, "warnings": list(dict.fromkeys(warnings))}


def _cap_rating_for_hard_exclusions(rating: Mapping[str, Any], exclusion_reasons: Sequence[str]) -> Dict[str, Any]:
    reasons = [str(item).strip() for item in exclusion_reasons if str(item).strip()]
    capped = dict(rating or {})
    if not reasons:
        return capped

    valuation_only = all(any(token in reason for token in ("估值", "价格位置")) for reason in reasons)
    target_rank = 1 if valuation_only else 0
    current_rank = int(capped.get("rank", 0) or 0)
    if current_rank > target_rank:
        capped["rank"] = target_rank
        if target_rank == 1:
            capped["label"] = "有信号但不充分"
            capped["meaning"] = "已触发硬排除，只能作为主线观察或风险提示，不能按正式推荐处理。"
            capped["stars"] = "⭐"
        else:
            capped["label"] = "无信号"
            capped["meaning"] = "已触发硬排除，当前不进入可执行推荐。"
            capped["stars"] = "—"
    warnings = list(capped.get("warnings") or [])
    warnings.append(f"⚠️ 已触发硬排除：{'；'.join(reasons[:3])}，结论只能按观察/风险提示处理")
    capped["warnings"] = list(dict.fromkeys(warnings))
    return capped


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
    *,
    asset_type: str = "",
    metadata: Mapping[str, Any] | None = None,
) -> str:
    direction = _direction_label(dimensions)
    odds = _odds_label(dimensions, metrics, technical)
    tech = _dimension_score(dimensions, "technical") or 0
    fundamental = _dimension_score(dimensions, "fundamental") or 0
    catalyst = _dimension_score(dimensions, "catalyst") or 0
    relative = _dimension_score(dimensions, "relative_strength") or 0
    risk = _dimension_score(dimensions, "risk") or 0
    relative_cross_check_failed = bool(dimensions.get("relative_strength", {}).get("cross_check_failed"))
    theme_confirmation_gate = _fund_like_theme_confirmation_gate(asset_type, metadata, dimensions)
    stock_signal_gate = _stock_signal_consistency_gate(asset_type, dimensions)

    if stock_signal_gate.get("applies"):
        return "观察为主"
    if direction in {"明确偏多", "中性偏多"} and odds == "低":
        return "持有优于追高"
    if theme_confirmation_gate.get("applies"):
        return "观察为主"
    if asset_type in {"cn_etf", "cn_fund", "cn_index"} and relative_cross_check_failed and (catalyst < 20 or tech < 50):
        return "观察为主"
    if asset_type == "cn_stock" and fundamental >= 80 and catalyst >= 15 and (relative >= 25 or risk >= 50) and tech >= 15:
        return "回调更优"
    if asset_type == "cn_stock" and fundamental >= 68 and (catalyst >= 18 or relative >= 25 or risk >= 50) and tech >= 15:
        return "等右侧确认"
    if asset_type in {"cn_etf", "cn_index"} and fundamental >= 45 and relative >= 40 and tech >= 20 and (catalyst >= 15 or risk >= 45):
        return "回调更优"
    if asset_type in {"cn_etf", "cn_index"} and fundamental >= 35 and relative >= 35 and tech >= 20 and (catalyst >= 15 or risk >= 40):
        return "等右侧确认"
    if fundamental >= 80 and catalyst >= 15 and relative >= 30 and tech >= 20:
        return "回调更优"
    if fundamental >= 60 and catalyst >= 25 and relative >= 25 and tech >= 20:
        return "等右侧确认"
    if risk >= 55 and fundamental >= 50 and relative >= 35 and tech >= 25:
        return "回调更优"
    if relative >= 60 and tech < 50:
        return "等右侧确认"
    if tech >= 50 and odds != "低":
        return "回调更优"
    if tech < 40:
        return "观察为主"
    return "风险释放前不宜激进"


def _client_safe_issue(label: str, exc: Any | None = None) -> str:
    text = str(exc or "").strip()
    lowered = text.lower()
    if any(token in lowered for token in ("too many requests", "rate limit", "429")):
        reason = "当前数据源限流，已按可用数据降级处理。"
    elif any(token in lowered for token in ("proxyerror", "connection", "timeout", "remote disconnected", "dns")):
        reason = "当前数据源连接不稳定，已按可用数据降级处理。"
    elif text:
        reason = "当前数据源暂不可用，已按可用数据降级处理。"
    else:
        reason = "当前数据暂不可用，已按可用数据降级处理。"
    return f"{label}: {reason}"


def _headline_core(
    dimensions: Mapping[str, Mapping[str, Any]],
    metrics: Mapping[str, float],
    technical: Mapping[str, Any],
    *,
    asset_type: str = "",
) -> str:
    tech = _dimension_score(dimensions, "technical") or 0
    catalyst = _dimension_score(dimensions, "catalyst") or 0
    relative = _dimension_score(dimensions, "relative_strength") or 0
    macro = _dimension_score(dimensions, "macro") or 0
    relative_cross_check_failed = bool(dimensions.get("relative_strength", {}).get("cross_check_failed"))
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    catalyst_dimension = dict(dimensions.get("catalyst") or {})
    coverage = dict(catalyst_dimension.get("coverage") or {})
    diagnosis = str(coverage.get("diagnosis", "")).strip()
    has_direct_company_evidence = bool(coverage.get("high_confidence_company_news")) or bool(
        coverage.get("effective_structured_event")
    )

    if asset_type == "cn_stock" and not has_direct_company_evidence and diagnosis in {
        "suspected_search_gap",
        "proxy_degraded",
        "theme_only_live",
        "stale_live_only",
    }:
        return "个股级新增证据还不够，当前更多只能先按行业背景和修复线索观察"
    if asset_type in {"cn_etf", "cn_fund", "cn_index"} and relative_cross_check_failed:
        return "短线相对强弱虽有修复，但中期业绩和样本长度还不支持升级为正式出手点"

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
    history: pd.DataFrame,
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
    trade_state = _trade_state_label(dimensions, metrics, technical, asset_type=asset_type, metadata=metadata)
    phase_label, phase_body = _phase_label(dimensions, technical)
    phase_headline = {
        "趋势启动": "逻辑与价格开始共振",
        "强势整理": "中期偏多，但短线略有拥挤",
        "防守轮动": "防守属性成立，但趋势尚未重新启动",
        "中期上行中的整理": "中期偏多，但短线仍在整理",
        "下行修复": "短期承压，仍在修复阶段",
        "震荡整理": "逻辑未破，但节奏一般",
    }.get(phase_label, "逻辑未破，但节奏一般")
    headline = f"这是一个**{phase_headline}**的标的。当前核心不是没逻辑，而是**{_headline_core(dimensions, metrics, technical, asset_type=asset_type)}**。"

    sector = str(metadata.get("sector", "综合"))
    theme = str(context.get("day_theme", {}).get("label", "背景宏观主导"))
    theme_context = _analysis_theme_playbook_context(metadata, context, fund_profile=fund_profile)
    playbook_label = str(theme_context.get("label", "")).strip()
    focus_exposure = str(metadata.get("industry_framework_label", "")).strip() or playbook_label or sector
    theme_alignment_level, matched_day_theme_label = _theme_alignment_match(metadata, dict(context.get("day_theme") or {}))
    explicit_theme = (
        matched_day_theme_label
        if theme_alignment_level == "direct" and matched_day_theme_label and matched_day_theme_label != "背景宏观主导"
        else ""
    )
    regime = str(context.get("regime", {}).get("current_regime", "unknown"))
    macro_score = _dimension_score(dimensions, "macro") or 0
    chips_dimension = dict(dimensions.get("chips") or {})
    chips_score = _dimension_score(dimensions, "chips")
    relative = dimensions["relative_strength"]
    relative_score = _dimension_score(dimensions, "relative_strength") or 0
    relative_cross_check_failed = bool(relative.get("cross_check_failed"))
    tech_score = _dimension_score(dimensions, "technical") or 0
    risk_score = _dimension_score(dimensions, "risk") or 0
    catalyst_score = _dimension_score(dimensions, "catalyst") or 0
    catalyst_dimension = dict(dimensions.get("catalyst") or {})
    catalyst_coverage = dict(catalyst_dimension.get("coverage") or {})
    catalyst_diagnosis = str(catalyst_coverage.get("diagnosis", "")).strip()
    has_direct_company_evidence = bool(catalyst_coverage.get("high_confidence_company_news")) or bool(
        catalyst_coverage.get("effective_structured_event")
    )
    fundamental_score = _dimension_score(dimensions, "fundamental") or 0
    fundamental_dimension = dimensions["fundamental"]
    valuation_snapshot = dict(fundamental_dimension.get("valuation_snapshot") or {})
    valuation_pe = valuation_snapshot.get("pe_ttm")
    valuation_note = str(fundamental_dimension.get("valuation_note", ""))
    price_percentile = float(metrics.get("price_percentile_1y", 0.5))
    commodity_like_fund = _is_commodity_like_fund(asset_type, metadata, fund_profile)
    support_signal = _find_factor(dimensions["technical"], "支撑位").get("signal", "关键支撑未明确")
    pressure_factor = _find_factor(dimensions["technical"], "压力位")
    pressure_signal = str(pressure_factor.get("signal", "")).strip()
    pressure_award = int(pressure_factor.get("awarded", 0) or 0)
    macd_signal = _find_factor(dimensions["technical"], "MACD 金叉").get("signal", "MACD 方向一般")
    candle_factor = _find_factor(dimensions["technical"], "K线形态")
    candle_signal = str(candle_factor.get("signal", "")).strip()
    candle_award = int(candle_factor.get("awarded", 0) or 0)
    divergence_factor = _find_factor(dimensions["technical"], "量价/动量背离")
    divergence_signal = str(divergence_factor.get("signal", "")).strip()
    divergence_award = int(divergence_factor.get("awarded", 0) or 0)
    false_break_factor = _find_factor(dimensions["technical"], "假突破识别")
    false_break_signal = str(false_break_factor.get("signal", "")).strip()
    false_break_award = int(false_break_factor.get("awarded", 0) or 0)
    compression_factor = _find_factor(dimensions["technical"], "压缩启动")
    compression_signal = str(compression_factor.get("signal", "")).strip()
    compression_award = int(compression_factor.get("awarded", 0) or 0)
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    ma20 = float(technical.get("ma_system", {}).get("mas", {}).get("MA20", 0.0))
    _support_label, support_level = _nearest_support_reference(history, technical)
    _, _, nearest_pressure_level = _pressure_signals(history, technical)
    macro_tilt = "偏顺风" if macro_score >= 28 else "大体中性" if macro_score >= 16 else "偏逆风"
    chip_winner_factor = _find_factor(chips_dimension, "筹码胜率")
    chip_cost_factor = _find_factor(chips_dimension, "平均成本位置")
    chip_pressure_factor = _find_factor(chips_dimension, "套牢盘压力")
    chip_distribution_factor = _find_factor(chips_dimension, "筹码密集区")
    capital_flow_factor = _find_factor(chips_dimension, "机构资金承接")
    margin_crowding_factor = _find_factor(chips_dimension, "两融拥挤度")
    broker_consensus_factor = _find_factor(dimensions["catalyst"], "卖方覆盖/一致预期")
    board_catalyst_factor = _find_factor(dimensions["catalyst"], "龙虎榜/打板确认")
    margin_risk_factor = _find_factor(dimensions["risk"], "两融拥挤")
    broker_crowding_factor = _find_factor(dimensions["risk"], "卖方一致预期过热")
    board_risk_factor = _find_factor(dimensions["risk"], "打板情绪风险")
    broker_consensus_signal = str(broker_consensus_factor.get("signal", "")).strip()
    broker_crowding_signal = str(broker_crowding_factor.get("signal", "")).strip()
    margin_risk_signal = str(margin_risk_factor.get("signal", "")).strip()
    board_risk_signal = str(board_risk_factor.get("signal", "")).strip()
    has_real_chip_signal = any(
        str(factor.get("signal", "")).strip() and str(factor.get("display_score", "")).strip() != "缺失"
        for factor in (chip_winner_factor, chip_cost_factor, chip_pressure_factor, chip_distribution_factor)
        if factor
    )

    if explicit_theme:
        macro_driver = (
            f"中期背景仍按 `{regime}` 处理，当天交易主线更偏 `{explicit_theme}`。对 `{focus_exposure}` 方向来说，宏观与主线整体{macro_tilt}，"
            f"问题不在于故事是否完全失效，而在于这种环境能否继续转化成新的价格确认。"
        )
    else:
        macro_driver = (
            f"当前中期背景更偏 `{regime}`。对 `{focus_exposure}` 方向来说，宏观整体{macro_tilt}，"
            f"问题不在于故事是否完全失效，而在于这种环境能否继续转化成新的价格确认。"
        )
    asset_note = _asset_note(metadata, asset_type).strip()
    if asset_note:
        macro_driver += f" {asset_note}"
    if asset_type == "cn_fund" and fund_profile:
        style_summary = str(dict(fund_profile.get("style") or {}).get("summary", "")).strip()
        if style_summary:
            if asset_note and "被动暴露" in asset_note and "被动暴露" in style_summary:
                label_tail = re.search(r"(当前标签是.+)$", style_summary)
                style_summary = label_tail.group(1).strip() if label_tail else ""
            if style_summary:
                normalized_asset_note = re.sub(r"[\s`，。；;,、:：]", "", asset_note)
                normalized_style_summary = re.sub(r"[\s`，。；;,、:：]", "", style_summary)
                if normalized_style_summary and normalized_style_summary in normalized_asset_note:
                    style_summary = ""
        if style_summary:
            macro_driver += f" {style_summary}"
    if commodity_like_fund:
        macro_driver += " 这类产品更接近商品/期货价格与期限结构的暴露，油价、库存、展期和保证金约束会比传统股票财报更重要。"

    if chips_score is None:
        flow_driver = "增量资金数据目前不完整，所以暂时看不到很强的机构加仓确认；当前更像配置属性或相对强弱在支撑。"
    elif chips_score >= 60:
        flow_driver = "资金面已经开始给出确认，说明这条线不只是讲故事，而是有增量资金在承接。"
    else:
        flow_driver = "资金面暂时没有形成明确共振，所以现阶段更多还是看方向和结构，而不是看资金追买。"
    if has_real_chip_signal:
        chip_clues = [
            str(factor.get("signal", "")).strip()
            for factor in (chip_winner_factor, chip_cost_factor, chip_pressure_factor, chip_distribution_factor)
            if str(factor.get("signal", "")).strip() and str(factor.get("display_score", "")).strip() != "缺失"
        ]
        if int(chip_pressure_factor.get("awarded", 0) or 0) < 0 or int(chip_cost_factor.get("awarded", 0) or 0) < 0:
            flow_driver = "真实筹码分布还在提示资金面没有完全站到这边，短线先按消化平均成本和上方套牢盘理解。"
        elif chips_score is not None and chips_score >= 60:
            flow_driver = "真实筹码分布已经开始配合价格，不只是行业代理在支撑，说明增量承接比之前更实。"
        else:
            flow_driver = "真实筹码分布还在拉锯，说明这条线不是没逻辑，而是筹码换手还没完全走到一边。"
        if chip_clues:
            flow_driver += f" 当前先盯 `{chip_clues[0]}`"
            if len(chip_clues) > 1:
                flow_driver += f" 和 `{chip_clues[1]}`"
            flow_driver += "。"
    capital_flow_signal = str(capital_flow_factor.get("signal", "")).strip()
    capital_flow_award = int(capital_flow_factor.get("awarded", 0) or 0)
    if asset_type == "cn_stock" and capital_flow_signal and capital_flow_signal != "缺失":
        if "个股主力净" in capital_flow_signal and capital_flow_award > 0:
            flow_driver = f"个股级资金流已经开始给出直接承接，不再只是行业代理在支撑。当前先看 `{capital_flow_signal}`。"
        elif "个股主力净" in capital_flow_signal and capital_flow_award < 0:
            flow_driver = f"个股级资金流仍在净流出，说明这条线短线更像逻辑在、资金没完全站过来。当前先看 `{capital_flow_signal}`。"
        elif capital_flow_award > 0:
            flow_driver += f" 目前还能看到 `{capital_flow_signal}`。"
    board_catalyst_signal = str(board_catalyst_factor.get("signal", "")).strip()
    board_catalyst_award = int(board_catalyst_factor.get("awarded", 0) or 0)
    if asset_type == "cn_stock" and board_catalyst_signal and board_catalyst_signal not in {"缺失", "未命中明确龙虎榜/打板确认"}:
        if board_catalyst_award > 0:
            flow_driver += f" 短线微观结构上，`{board_catalyst_signal}` 也在配合。"
        elif board_catalyst_award < 0:
            flow_driver += f" 但 `龙虎榜/打板确认` 当前显示 `{board_catalyst_signal}`，说明催化很容易先被情绪风险打断。"
    if asset_type == "cn_stock" and broker_consensus_signal and broker_consensus_signal not in {"缺失", "本月未命中明确券商金股推荐"} and "最新卖方金股仍停在" not in broker_consensus_signal:
        flow_driver += f" 卖方侧当前还能看到 `{broker_consensus_signal}`，但这里只把它当共识热度参考，不替代公司级强催化。"
    if asset_type == "cn_fund" and fund_profile:
        positioning = str(dict(fund_profile.get("style") or {}).get("positioning", "")).strip()
        if positioning:
            flow_driver += f" {positioning}"

    relative_proxy_only = bool(relative.get("proxy_only"))
    if relative_cross_check_failed:
        relative_driver = "短线相对强弱虽然有修复，但同类业绩和样本长度的交叉校验还不支持把它写成轮动确认。"
    elif relative_proxy_only:
        if relative_score >= 40:
            relative_driver = "相对强弱有改善，但行业宽度和龙头确认仍缺失，当前更适合作为低置信代理去看，而不是把它写成完整扩散确认。"
        else:
            relative_driver = "相对强弱偏弱，且行业宽度和龙头确认仍缺失，更适合先当成背景观察，而不是把它当成轮动已经回来的证明。"
    elif relative_score >= 70:
        relative_driver = "相对强弱仍占优，说明资金没有彻底离开这条线；即使短线有波动，也更像强势方向内部的节奏调整。"
    elif relative_score >= 40:
        relative_driver = "相对强弱处在中间地带，说明它还没有被市场完全放弃，但也不是当前最强的扩散方向。"
    else:
        relative_driver = "相对强弱偏弱，说明当前轮动还没有明确回到它身上，更多是修复观察而不是主线确认。"
    if catalyst_score < 10 and relative_score >= 70:
        relative_driver = (
            "相对强弱分数当前还在，但更多是在反映前一段主线和价格惯性的滞后优势；"
            "如果新增直接情报继续缺位，这个高分本身也容易先回落。"
        )

    if tech_score >= 60:
        technical_driver = f"技术结构整体完整，`{macd_signal}`，中期趋势没有被破坏。"
    elif support_signal and support_signal != "当前价格未明显贴近 MA60、前低或关键斐波那契支撑":
        technical_driver = f"技术面最值得看的不是强趋势，而是价格已经回到 `{support_signal}` 附近；但 `{macd_signal}`，短线动能还需要再修复。"
    else:
        technical_driver = f"技术面当前最大问题不是完全破位，而是 `{macd_signal}`，趋势确认不足。"
    if divergence_signal and "未识别到明确顶/底背离" not in divergence_signal:
        if divergence_award > 0:
            technical_driver += f" 同时出现 `{divergence_signal}`，说明价格回撤和动量/量能没有同步恶化，这更像止跌修复的辅助确认。"
        elif divergence_award < 0:
            technical_driver += f" 当前还叠加 `{divergence_signal}`，说明价格和动量/量能并没有完全同步，短线更需要等背离消化。"
    if candle_signal and "无明确组合形态" not in candle_signal:
        if candle_award >= 7:
            technical_driver += f" 最近 1-3 根 K 线还出现 `{candle_signal}`，说明短线承接开始改善。"
        elif candle_award <= -7:
            technical_driver += f" 但最近 1-3 根 K 线出现 `{candle_signal}`，说明反弹结构还没有真正站稳。"
    if false_break_signal and "未识别到明确假突破" not in false_break_signal:
        if false_break_award > 0:
            technical_driver += f" 同时出现 `{false_break_signal}`，空头试探失败是多头的辅助确认。"
        elif false_break_award < 0:
            technical_driver += f" 同时出现 `{false_break_signal}`，多头试探失败说明突破位上方承压仍重。"
    if compression_signal and "中性" not in compression_signal:
        if compression_award >= 8:
            technical_driver += f" 当前处于 `{compression_signal}`，是相对干净的介入 setup。"
        elif compression_award < 0:
            technical_driver += f" 但当前处于 `{compression_signal}`，追涨赔率偏低。"
    if pressure_signal:
        if pressure_award < 0:
            technical_driver += f" 同时上方还要先消化 `{pressure_signal}`，所以反弹更像先处理承压，而不是直接进入顺畅加速。"
        elif pressure_award > 0 and "上方最近明确压力不近" in pressure_signal:
            technical_driver += " 上方最近没有很近的压制位，说明一旦动能修复，价格还有继续试探的空间。"
    if asset_type == "cn_stock" and board_catalyst_signal and board_catalyst_signal not in {"缺失", "未命中明确龙虎榜/打板确认"}:
        if board_catalyst_award > 0:
            technical_driver += f" 盘后微观结构还出现 `{board_catalyst_signal}`，说明情绪面和交易席位也在给短线确认。"
        elif board_catalyst_award < 0:
            technical_driver += f" 但微观结构显示 `{board_catalyst_signal}`，短线更要防情绪交易先反噬。"
    if asset_type == "cn_fund" and fund_profile:
        selection = str(dict(fund_profile.get("style") or {}).get("selection", "")).strip()
        if selection:
            technical_driver += f" {selection}"
    if asset_type in {"cn_etf", "cn_fund"}:
        index_snapshot = dict(
            metadata.get("index_technical_snapshot")
            or dict(metadata.get("index_topic_bundle") or {}).get("technical_snapshot")
            or {}
        )
        index_trend = str(index_snapshot.get("trend_label", "")).strip()
        index_momentum = str(index_snapshot.get("momentum_label", "")).strip()
        fund_trend = str(metadata.get("fund_factor_trend_label", "")).strip()
        fund_momentum = str(metadata.get("fund_factor_momentum_label", "")).strip()
        if index_trend in {"修复中", "趋势偏强"} and fund_trend == "趋势偏弱":
            technical_driver += (
                " 还要承认一个分歧：跟踪指数端更像 `"
                + index_trend
                + (" / " + index_momentum if index_momentum else "")
                + "`，但产品层技术因子还是 `"
                + fund_trend
                + (" / " + fund_momentum if fund_momentum else "")
                + "`；赛道背景先看指数，真正执行仍先以产品层修复为准。"
            )

    if macro_score >= 30 and relative_score >= 60 and tech_score < 50:
        contradiction = "中期逻辑偏正面，但短线动能还没有重新修复，因此更适合等待确认，而不是直接追价。"
    elif asset_type == "cn_stock" and (
        "融资盘升温明显" in margin_risk_signal
        or "情绪交易升温" in board_risk_signal
        or "龙虎榜/跌停/竞价信号偏负面" in board_risk_signal
    ):
        contradiction = "题材和交易结构并不是完全没亮点，但两融/打板情绪已经开始升温，导致‘看对方向’和‘执行节奏舒服’之间出现了新的矛盾。"
    elif asset_type == "cn_stock" and broker_crowding_signal and broker_crowding_signal not in {"缺失", "当前卖方共识不构成明确过热结论"}:
        contradiction = "卖方共识并不弱，但券商月度金股覆盖已经开始变密，导致‘逻辑在升温’和‘赔率是否还舒服’之间出现了新的矛盾。"
    elif catalyst_score >= 50 and (price_percentile >= 0.80 or (valuation_pe is not None and float(valuation_pe) >= 45)):
        contradiction = "催化并不弱，但市场已经提前交易了大半预期，所以现在的核心矛盾不是‘有没有故事’，而是‘还有没有足够好的赔率’。"
    elif price_percentile >= 0.85 and (relative_score >= 55 or macro_score >= 30):
        contradiction = "方向并不差，但价格已经处在偏高位置，导致‘逻辑正确’和‘位置舒服’之间出现了明显错位。"
    elif asset_type == "cn_stock" and not has_direct_company_evidence and catalyst_diagnosis in {
        "suspected_search_gap",
        "proxy_degraded",
        "theme_only_live",
        "stale_live_only",
    }:
        contradiction = "当前最大的矛盾不是价格先坏了，而是个股级新增证据还不够，只能先按行业背景和修复线索观察。"
    elif catalyst_score < 30 and tech_score >= 45:
        contradiction = "价格结构不算差，但缺少新的催化去推动第二阶段上涨，所以现在更像磨时间，而不是直接加速。"
    else:
        contradiction = "当前最大的矛盾在于逻辑并非彻底失效，但还缺少足够清晰的价格、资金和催化共振。"
    if catalyst_score < 10 and relative_score >= 70:
        contradiction = "相对强弱分数还在，但它更多是在反映前一段主线惯性；新增直接情报没回来前，别把这个高分直接读成新一轮确认。"

    positives: List[str] = []
    if macro_score >= 30:
        if explicit_theme:
            positives.append(f"中期 `{regime}` 背景仍在，当天主线 `{explicit_theme}` 也没有把这个方向推成纯宏观逆风。")
        else:
            positives.append(f"当前 `{regime}` 背景下，`{focus_exposure}` 这条线至少没有明显宏观逆风。")
    if relative_score >= 60:
        positives.append("相对强弱仍占优，说明它不是市场最先被放弃的方向。")
    if risk_score >= 60:
        positives.append("回撤、相关性或防守属性还算可控，适合放进组合框架里评估。")
    if support_signal and support_signal != "当前价格未明显贴近 MA60、前低或关键斐波那契支撑":
        positives.append(f"价格靠近 `{support_signal}`，说明下方不是完全没有承接。")
    if pressure_award > 0 and pressure_signal and "上方最近明确压力不近" in pressure_signal:
        positives.append("上方最近明确压力不近，说明反弹不是一抬头就先撞线。")
    if has_real_chip_signal and int(chip_cost_factor.get("awarded", 0) or 0) >= 0 and int(chip_pressure_factor.get("awarded", 0) or 0) >= 0:
        positives.append("真实筹码分布没有显示明显套牢盘压制，价格修复时更容易形成顺畅承接。")
    if asset_type == "cn_stock" and capital_flow_award > 0 and capital_flow_signal:
        positives.append(f"个股/主题资金流开始给出承接，当前能看到 `{capital_flow_signal}`。")
    if asset_type == "cn_stock" and broker_consensus_signal and broker_consensus_signal not in {"缺失", "本月未命中明确券商金股推荐"} and "最新卖方金股仍停在" not in broker_consensus_signal:
        positives.append(f"卖方覆盖没有掉线，当前还能看到 `{broker_consensus_signal}`。")
    if asset_type == "cn_stock" and board_catalyst_award > 0 and board_catalyst_signal:
        positives.append(f"微观交易结构开始配合，`{board_catalyst_signal}` 说明短线催化不只是口头逻辑。")
    if not positives:
        positives.append("当前仍保留一定观察价值，主要因为趋势并未被彻底破坏。")

    cautions: List[str] = []
    if fundamental_score <= 20:
        cautions.append("估值/基本面安全边际并不突出，当前价格已经提前反映一部分预期。")
    if catalyst_score < 30:
        cautions.append("缺少新的催化去推动下一段行情，短线更难靠故事继续推升。")
    if catalyst_score < 10 and relative_score >= 70:
        cautions.append("相对强弱当前更多是滞后优势；如果新增直接情报继续缺位，高分本身也会先回落。")
    if commodity_like_fund:
        cautions.append("商品/期货 ETF 还要额外防展期损益和期限结构变化，方向看对也不等于净值会完全同步现货。")
    if tech_score < 50:
        cautions.append("短线动能不足，趋势确认仍欠缺。")
    if asset_type in {"cn_etf", "cn_fund"}:
        index_snapshot = dict(
            metadata.get("index_technical_snapshot")
            or dict(metadata.get("index_topic_bundle") or {}).get("technical_snapshot")
            or {}
        )
        index_trend = str(index_snapshot.get("trend_label", "")).strip()
        index_momentum = str(index_snapshot.get("momentum_label", "")).strip()
        fund_trend = str(metadata.get("fund_factor_trend_label", "")).strip()
        fund_momentum = str(metadata.get("fund_factor_momentum_label", "")).strip()
        if index_trend in {"修复中", "趋势偏强"} and fund_trend == "趋势偏弱":
            cautions.append(
                "跟踪指数在修复，但产品层技术因子仍偏弱；赛道背景和执行载体有分歧，真要动手先以产品层修复为准。"
                + (f" 当前分歧是 `{index_trend}" + (f" / {index_momentum}" if index_momentum else "") + f"` vs `{fund_trend}" + (f" / {fund_momentum}" if fund_momentum else "") + "`。")
            )
    if rsi > 70:
        cautions.append("短线已经偏拥挤，即使逻辑不坏，追高的盈亏比也不优。")
    if pressure_award < 0 and pressure_signal:
        cautions.append(f"上方还要先消化 `{pressure_signal}`，反弹不等于已经进入顺畅加速。")
    if has_real_chip_signal and (int(chip_pressure_factor.get("awarded", 0) or 0) < 0 or int(chip_cost_factor.get("awarded", 0) or 0) < 0):
        cautions.append("真实筹码分布仍提示平均成本压制或上方套牢盘偏重，反弹先按消化抛压理解。")
    if asset_type == "cn_stock" and ("融资盘升温明显" in margin_risk_signal or "融资盘仍在升温" in margin_risk_signal):
        cautions.append("两融资金正在升温，一旦共识反转，融资盘会先放大短线回撤。")
    if asset_type == "cn_stock" and broker_crowding_signal and broker_crowding_signal not in {"缺失", "当前卖方共识不构成明确过热结论"}:
        cautions.append("券商月度金股覆盖已经偏密，后续更需要新的业绩/订单验证，而不是只靠卖方共识续推。")
    if asset_type == "cn_stock" and ("情绪交易升温" in board_risk_signal or "龙虎榜/跌停/竞价信号偏负面" in board_risk_signal):
        cautions.append("龙虎榜/打板结构提示情绪交易风险抬升，短线更要防高开低走或次日承接不足。")
    if not cautions:
        cautions.append("当前最大问题不是方向，而是节奏，仍要防止追在情绪高点。")

    external_risk_map = {
        "半导体": "需要继续盯利率、美元、AI 资本开支、晶圆厂 capex 和存储价格周期；一旦高估值逻辑失去新增验证，回撤会来得很快。",
        "黄金": "需要继续盯美元、实际利率和地缘溢价；一旦美元转强且避险交易降温，价格回吐会很快。",
        "科技": "需要继续盯利率、美元和风险偏好；只要波动率抬升，估值就容易再受压。",
        "电网": "需要继续盯政策节奏、商品价格和风险偏好；若主线切走，强势方向也可能进入获利了结。",
        "能源": "需要继续盯油价、地缘和政策调控；一旦油价冲高回落，交易拥挤可能迅速反噬。",
        "有色": "需要继续盯美元、商品价格和全球增长预期；外部需求转弱时弹性会明显收缩。",
        "农业": "需要继续盯天气、粮价、化肥成本和政策节奏；如果粮食安全和农资价格主线降温，主题溢价回吐也会很快。",
    }
    company_name = str(metadata.get("name", analysis_seed.get("symbol")))
    stock_like = asset_type in {"cn_stock", "hk", "us"}
    company_specific_fundamental_risk = stock_like and fundamental_score <= 25
    risk_points = {
        "fundamental": (
            f"{company_name} 自身的盈利质量、兑现节奏和估值承压已经是当前基本面风险的一部分；即使 `{focus_exposure}` 景气没有继续恶化，"
            "公司端如果拿不出更强的经营兑现，估值支撑也会继续下移。"
            if company_specific_fundamental_risk
            else f"真正的基本面风险不在 {company_name} 本身，而在其所暴露的 `{focus_exposure}` 景气如果不及预期，估值支撑会继续下移。"
        ),
        "valuation": (
            f"当前{('个股估值' if asset_type in {'cn_stock', 'hk', 'us'} else valuation_snapshot.get('display_label', '真实指数估值'))}参考为 `{valuation_snapshot.get('index_name', '相关指数')}` "
            f"{valuation_snapshot.get('metric_label', '滚动PE')} `{float(valuation_pe):.1f}x`，"
            f"同时价格位置在近一年 `{price_percentile:.0%}` 分位；高估值和高位置是两层风险，不是一回事。"
            if valuation_pe is not None
            else f"当前价格位置大约在近一年 `{price_percentile:.0%}` 分位。{valuation_note}"
        ),
        "crowding": "如果这条线继续成为市场共识，短线资金拥挤会放大波动；如果共识撤退，回撤也会更陡。" if (relative_score >= 60 or rsi > 65) else "当前拥挤风险不算极端，但一旦没有增量资金确认，走势容易反复。",
        "external": external_risk_map.get(playbook_label or sector, external_risk_map.get(sector, "还要继续盯利率、美元、商品价格和市场风格切换，这类外部变量往往会比基本面更快改写短线定价。")),
    }
    if has_real_chip_signal and int(chip_pressure_factor.get("awarded", 0) or 0) < 0:
        risk_points["crowding"] = "真实筹码分布显示上方套牢盘仍偏重，一旦承接转弱，回撤容易被解套盘放大。"
    if asset_type == "cn_stock" and ("融资盘升温明显" in margin_risk_signal or "融资盘仍在升温" in margin_risk_signal):
        risk_points["crowding"] = "两融资金正在升温，融资盘一致性交易会放大波动；一旦主线回撤，短线更容易被被动卖压放大。"
    if asset_type == "cn_stock" and broker_crowding_signal and broker_crowding_signal not in {"缺失", "当前卖方共识不构成明确过热结论"}:
        risk_points["crowding"] = "券商月度金股覆盖已经偏密，卖方一致预期过热时，后续更需要新的订单/业绩验证；否则更容易先交易共识回落。"
    if asset_type == "cn_stock" and ("情绪交易升温" in board_risk_signal or "龙虎榜/跌停/竞价信号偏负面" in board_risk_signal):
        risk_points["crowding"] = "打板/龙虎榜结构提示短线情绪交易偏热或偏负面，次日承接一旦不足，波动会先被情绪盘放大。"
    if commodity_like_fund:
        risk_points["fundamental"] = (
            f"真正的基本面风险不在 {metadata.get('name', analysis_seed.get('symbol'))} 本身，而在其跟踪的商品/期货指数方向、"
            "合约展期损益、保证金约束和跟踪误差。"
        )
        risk_points["external"] = (
            "除了油价、地缘和政策调控，还要继续盯期限结构、库存预期和展期损益；方向看对时，净值也可能因为期货曲线和保证金结构而弱于现货直觉。"
        )

    watch_points = [
        f"短线动能是否重新修复：重点看 `{macd_signal}` 能否扭转，或价格重新站上关键均线。",
        f"关键支撑是否有效：重点看 `{support_signal}` 附近是否出现企稳，而不是继续失守。",
        f"近端压力是否消化：重点看 `{pressure_signal or '上方近端压力'}` 能否被放量突破，而不是一反弹就遇阻回落。",
        "资金是否重新形成共振：ETF 份额、主力资金或相关配置资金是否重新转正。",
        (
            f"主线变量是否延续：继续观察 `{explicit_theme}` 是否强化，以及它对应的宏观变量是否继续配合。"
            if explicit_theme
            else (
                f"主线变量是否延续：继续观察 `{focus_exposure}` 这条线是否重新强化，以及它对应的订单、估值和风格变量是否继续配合。"
                if playbook_label
                else f"主线变量是否延续：继续观察 `{theme}` 是否强化，以及它对应的宏观变量是否继续配合。"
            )
        ),
    ]
    if has_real_chip_signal:
        watch_points[3] = "真实筹码结构是否继续改善：重点看平均成本能否继续回到现价下方、上方套牢盘占比是否继续收敛。"
    elif asset_type == "cn_stock" and capital_flow_signal:
        watch_points[3] = f"个股资金是否继续承接：重点看 `{capital_flow_signal}` 能否延续，而不是只剩行业代理在支撑。"
    if asset_type == "cn_stock" and ("融资盘升温明显" in margin_risk_signal or "融资盘仍在升温" in margin_risk_signal):
        watch_points[3] = "两融拥挤度是否降温：重点看融资余额增速和融资买入/偿还比能否回落，避免融资盘继续堆高短线拥挤。"
    if asset_type == "cn_stock" and broker_consensus_signal and broker_consensus_signal not in {"缺失", "本月未命中明确券商金股推荐"}:
        watch_points.append("卖方覆盖是否继续扩散：重点看下月券商金股新增覆盖能否延续，而不是只剩旧共识堆积。")

    validation_points = [
        {
            "watch": "短线动能重启",
            "judge": f"MACD 重新金叉，且收盘站回 MA20 `{ma20:.3f}` 上方",
            "bull": "说明价格开始从整理转向确认，趋势交易者可以重新评估右侧介入。",
            "bear": "说明仍处于弱修复或横盘，继续等确认而不是抢跑。",
        },
        {
            "watch": "关键支撑是否守住",
            "judge": (
                f"收盘不低于关键支撑 `{support_level:.3f}` 下方 2%"
                if support_level > 0
                else "近端低点不再被连续跌破"
            ),
            "bull": "说明回调更像消化而不是破位，左侧观察价值仍在。",
            "bear": "说明支撑失效，先处理风险，再谈逻辑。",
        },
        {
            "watch": "近端压力是否突破",
            "judge": (
                f"收盘站上近端压力 `{nearest_pressure_level:.3f}`，且次日不回落失守"
                if nearest_pressure_level > 0
                else "价格继续抬高并站稳近端高点，上方没有新的明显承压"
            ),
            "bull": "说明上方抛压开始被消化，反弹更有机会升级成趋势确认。",
            "bear": "说明还是先遇阻回落，当前更像区间内反弹或修复波段。",
        },
        {
            "watch": "资金是否回流",
            "judge": (
                "近 5 个可用样本里，个股主力净流入/行业主力净流入至少 3 次转正"
                if asset_type == "cn_stock"
                else "近 5 个可用样本里，ETF份额/主力净流入至少 3 次转正"
                if asset_type in {"cn_etf", "cn_fund"}
                else "近 5 个可用样本里，相关配置资金至少 3 次转正"
            ),
            "bull": "说明不只是逻辑在，资金也开始重新确认。",
            "bear": "说明还是存量博弈，价格容易反复。",
        },
    ]
    if playbook_label == "半导体":
        validation_points.append(
            {
                "watch": "成长估值逆风是否缓和",
                "judge": "VIX 回落到 25 以下，且 DXY 5日不再继续走强",
                "bull": "说明成长估值压力缓解，半导体这类高 beta 成长方向更容易从修复走向扩散。",
                "bear": "说明宏观仍逆风，高估值半导体方向继续受压。",
            }
        )
    elif sector == "科技":
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

    summary_intro = (
        f"总体来看，`{metadata.get('name', analysis_seed.get('symbol'))}` 的核心逻辑在于 `{explicit_theme}` 主线下的 `{focus_exposure}` 暴露仍有跟踪价值；"
        if explicit_theme
        else (
            f"总体来看，`{metadata.get('name', analysis_seed.get('symbol'))}` 的核心逻辑在于 `{focus_exposure}` 这条线的中期逻辑仍未完全失效；"
            if playbook_label
            else f"总体来看，`{metadata.get('name', analysis_seed.get('symbol'))}` 的核心逻辑在于 `{theme}` 背景下的 `{sector}` 暴露仍有配置价值；"
        )
    )
    summary_lines = [
        summary_intro,
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
) -> Dict[str, Any]:
    rating = analysis["rating"]["rank"]
    asset_type = str(analysis.get("asset_type", ""))
    metadata = dict(analysis.get("metadata") or {})
    tech = analysis["dimensions"]["technical"]["score"]
    fundamental_score = analysis["dimensions"]["fundamental"]["score"] or 0
    risk_score = analysis["dimensions"]["risk"]["score"] or 0
    relative_score = analysis["dimensions"]["relative_strength"]["score"] or 0
    catalyst_score = analysis["dimensions"]["catalyst"]["score"] or 0
    macro_reverse = analysis["dimensions"]["macro"].get("macro_reverse", False)
    rsi = float(technical.get("rsi", {}).get("RSI", 50.0))
    divergence = dict(technical.get("divergence") or {})
    divergence_signal = str(divergence.get("signal", "neutral"))
    divergence_age_days = _divergence_signal_age_days(divergence, history["date"].iloc[-1])
    if divergence_signal != "neutral" and divergence_age_days is not None and divergence_age_days > 7:
        divergence_signal = "neutral"
    candlestick_patterns = set(technical.get("candlestick", []) or [])
    bearish_candle = any(
        pattern in candlestick_patterns
        for pattern in {"evening_star", "three_black_crows", "three_inside_down", "bearish_engulfing", "bearish_harami", "dark_cloud_cover", "tweezer_top", "shooting_star", "hanging_man", "bearish_marubozu"}
    )
    bullish_candle = any(
        pattern in candlestick_patterns
        for pattern in {"morning_star", "three_white_soldiers", "three_inside_up", "bullish_engulfing", "bullish_harami", "piercing_line", "tweezer_bottom", "hammer", "inverted_hammer", "bullish_marubozu"}
    )
    fib_levels = technical.get("fibonacci", {}).get("levels", {})
    ma20 = float(technical.get("ma_system", {}).get("mas", {}).get("MA20", history["close"].iloc[-1]))
    ma60 = float(technical.get("ma_system", {}).get("mas", {}).get("MA60", history["close"].iloc[-1]))
    close_now = float(history["close"].iloc[-1])
    ma20_gap = ((close_now / ma20) - 1.0) if ma20 else 0.0
    vol_percentile = float((metrics or {}).get("volatility_percentile_1y", 0.5))
    return_5d = float((metrics or {}).get("return_5d", 0.0))
    price_percentile_1y = float((metrics or {}).get("price_percentile_1y", 0.5))
    theme_confirmation_gate = _fund_like_theme_confirmation_gate(asset_type, metadata, analysis["dimensions"])
    trade_state = _trade_state_label(
        analysis["dimensions"],
        metrics or {},
        technical,
        asset_type=asset_type,
        metadata=metadata,
    )
    etf_trend_continuation = (
        asset_type in {"cn_etf", "cn_fund"}
        and not theme_confirmation_gate.get("applies")
        and not macro_reverse
        and tech is not None
        and tech >= 48
        and relative_score >= 65
        and risk_score >= 45
        and (fundamental_score >= 40 or catalyst_score >= 40)
    )

    # Read setup signals from technical scorecard
    setup_block = dict(technical.get("setup") or {})
    false_break_kind = str(dict(setup_block.get("false_break") or {}).get("kind", "none"))
    false_break_award = -8 if false_break_kind == "bullish_false_break" else 8 if false_break_kind == "bearish_false_break" else 0
    compression_kind = str(dict(setup_block.get("compression_setup") or {}).get("kind", "neutral"))
    compression_award = 10 if compression_kind == "compression_breakout" else -5 if compression_kind == "momentum_chase" else 0
    phase_label, _phase_body = _phase_label(analysis["dimensions"], technical)
    pressure_award = int(_find_factor(analysis["dimensions"]["technical"], "压力位").get("awarded", 0) or 0)
    computed_pressure_award, _pressure_detail, nearest_pressure_level = _pressure_signals(history, technical)
    if pressure_award == 0:
        pressure_award = computed_pressure_award
    stock_signal_gate = _stock_signal_consistency_gate(asset_type, analysis["dimensions"])

    if theme_confirmation_gate.get("applies"):
        direction = "观察为主"
    elif stock_signal_gate.get("applies"):
        target_rank = int(stock_signal_gate.get("target_rank") if stock_signal_gate.get("target_rank") is not None else 1)
        direction = "回避" if target_rank <= 0 or rating <= 0 else "观望"
    elif rating >= 3:
        if macro_reverse:
            direction = "观望偏多"
        elif trade_state in {"持有优于追高", "风险释放前不宜激进", "观察为主"}:
            direction = "观望偏多"
        elif trade_state == "等右侧确认" and not (
            (
                asset_type == "cn_stock"
                and fundamental_score >= 68
                and (catalyst_score >= 18 or relative_score >= 25 or risk_score >= 50)
                and tech >= 15
            )
            or (
                asset_type in {"cn_etf", "cn_index"}
                and fundamental_score >= 35
                and relative_score >= 35
                and tech >= 20
                and (catalyst_score >= 15 or risk_score >= 40)
            )
            or (
                fundamental_score >= 70 and (catalyst_score >= 25 or relative_score >= 45 or risk_score >= 55)
            )
        ):
            direction = "观望偏多"
        else:
            direction = "做多"
    elif etf_trend_continuation:
        direction = "观望偏多"
    elif rating == 2:
        direction = "观望"
    elif risk_score >= 70 and relative_score >= 60:
        direction = "观望偏多"
    else:
        direction = "回避"

    # --- Entry conditions: incorporate risk and relative strength ---
    if theme_confirmation_gate.get("applies"):
        entry = "主题主线还缺技术/催化确认，先放在观察名单，等价格与情报一起补齐后再讨论第一笔。"
    elif stock_signal_gate.get("applies"):
        entry = "技术、催化或风险硬门槛还没过，今天不设买点；至少等技术修复、直接催化或风险收益比补上一项再重评。"
    elif divergence_signal == "bearish" or bearish_candle or false_break_award < 0:
        entry = "先等顶背离/假突破消化、MACD/OBV 重新同步，再考虑分批介入"
    elif rsi > 70 or compression_award < 0:
        entry = "等 RSI 回落到 60 附近且 MACD 不死叉，当前情绪追价区赔率偏低，不追高"
    elif compression_award >= 8:
        entry = "当前处于压缩后放量启动 setup，可在量能持续确认后小仓试探，止损设在压缩区低点"
    elif (divergence_signal == "bullish" or bullish_candle) and tech is not None and tech >= 40:
        entry = "已有底背离雏形，更适合等价格重新站回 MA20 或 MACD 继续走强后小仓试探"
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
    if theme_confirmation_gate.get("applies"):
        position = "先按观察仓理解，不预设正式建仓"
    elif stock_signal_gate.get("applies") and rating >= 1:
        position = "不设正式建仓；若已有仓位，按观察仓或风控线管理，不用新增资金去验证。"
    elif rating >= 3:
        if tech is not None and tech >= 70:
            position = "首次建仓 ≤8%，确认突破后可加到 15%"
        elif tech is not None and tech >= 55:
            position = "首次建仓 ≤5%，确认后再加到 10%"
        else:
            position = "首次建仓 ≤3%，等结构进一步确认后再加仓"
    elif etf_trend_continuation:
        position = "首次建仓 ≤3%，右侧确认或回踩承接后再加到 8%"
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
        base_stop_loss_pct = "-5%"
    elif vol_percentile <= 0.60:
        base_stop_loss_pct = "-8%"
    else:
        base_stop_loss_pct = "-10%"

    stop_buffer = abs(float(base_stop_loss_pct.strip("%"))) / 100.0
    atr_stop_gap = _minimum_stop_gap_from_atr(history, technical, asset_type=asset_type)
    required_stop_gap = max(stop_buffer, atr_stop_gap)
    support_candidates = [level for _, _, level in _support_reference_candidates(history, technical)]
    structural_stop = max(support_candidates) * 0.995 if support_candidates else 0.0
    stop_floor = close_now * (1.0 - stop_buffer)
    stop_ref = max(structural_stop, stop_floor) if structural_stop else stop_floor
    if stop_ref >= close_now:
        stop_ref = stop_floor
    min_validation_gap = 0.02 if asset_type in {"hk", "us"} else 0.01
    if required_stop_gap > 0 and stop_ref > close_now * (1.0 - required_stop_gap):
        stop_ref = close_now * (1.0 - required_stop_gap)
    if stop_ref >= close_now * (1.0 - max(min_validation_gap, required_stop_gap)):
        stop_ref = close_now * (1.0 - max(required_stop_gap, min_validation_gap))

    target_floor = close_now * (1.12 if rating >= 3 else 1.08)
    resistance_candidates = [
        candidate
        for candidate in [
            float(history["high"].tail(60).max()),
            float(fib_levels.get("1.000", 0.0)),
        ]
        if candidate and candidate > close_now
    ]
    prefer_first_pressure = (
        pressure_award < 0
        and nearest_pressure_level > close_now * 1.001
        and (
            rating <= 2
            or direction in {"回避", "观望", "观察为主"}
            or asset_type in {"cn_etf", "cn_fund", "cn_index"}
        )
    )
    first_pressure_ref = float(nearest_pressure_level) if prefer_first_pressure else 0.0
    target_ref = first_pressure_ref or (max([target_floor, *resistance_candidates]) if resistance_candidates else target_floor)
    if target_ref <= close_now:
        target_ref = target_floor
    if stop_ref >= close_now:
        stop_ref = close_now * (1.0 - max(required_stop_gap, min_validation_gap))
    if target_ref <= close_now:
        target_ref = close_now * 1.05

    buy_low_ref: Optional[float] = None
    buy_high_ref: Optional[float] = None
    buy_range_note = ""
    if not (direction == "回避" and "暂不出手" in position):
        buy_candidates = [
            candidate
            for candidate in [
                ma20,
                ma60,
                float(fib_levels.get("0.382", 0.0)),
                float(fib_levels.get("0.500", 0.0)),
                float(fib_levels.get("0.618", 0.0)),
            ]
            if candidate > 0 and candidate <= close_now * 1.03
        ]
        if tech is not None and tech >= 55:
            buy_anchor = max(
                [candidate for candidate in buy_candidates if candidate <= close_now * 1.01],
                default=min(close_now, ma20 or close_now),
            )
        elif etf_trend_continuation or rating >= 2 or relative_score >= 60 or risk_score >= 70:
            buy_anchor = max(
                [candidate for candidate in buy_candidates if candidate <= close_now],
                default=close_now * 0.99,
            )
        else:
            buy_anchor = 0.0
        if buy_anchor > 0:
            buy_high_candidate = min(close_now * 1.005, max(buy_anchor * 1.01, close_now * 0.985))
            min_entry_stop_gap = max(0.015 if asset_type in {"hk", "us"} else 0.01, stop_buffer * 0.10)
            min_buy_low = stop_ref / max(1.0 - min_entry_stop_gap, 0.01)
            buy_low_candidate = max(min_buy_low, min(buy_anchor, buy_high_candidate) * 0.985)
            if buy_low_candidate < buy_high_candidate:
                buy_low_ref = buy_low_candidate
                buy_high_ref = buy_high_candidate
            else:
                buy_range_note = (
                    f"暂不设，当前候选买点离止损位太近（至少留 `{min_entry_stop_gap:.1%}` 缓冲）,"
                    " 先等回踩更深或右侧确认后再给区间。"
                )

    trim_low_ref = max(close_now * (1.06 if rating >= 3 else 1.04), target_ref * 0.97)
    trim_high_ref = max(trim_low_ref * 1.02, target_ref * 1.03)
    if trim_high_ref <= trim_low_ref:
        trim_high_ref = trim_low_ref * 1.02

    timeframe = (
        "中线配置(1-3月)"
        if rating >= 3
        else "波段跟踪(2-6周)"
        if etf_trend_continuation
        else "短线交易(1-2周)"
        if rating >= 2 or (risk_score >= 70 and relative_score >= 60)
        else "等待更好窗口"
    )
    actual_stop_gap = max((close_now - stop_ref) / close_now, 0.0) if close_now else 0.0
    stop_loss_pct = f"-{max(int(round(actual_stop_gap * 100)), 1)}%"
    target = (
        f"先看近端压力 {target_ref:.3f} 能否放量消化；站稳后再看更远目标"
        if first_pressure_ref
        else f"先看前高/近 60 日高点 {target_ref:.3f} 附近的承压与突破情况"
    )
    if atr_stop_gap > stop_buffer + 0.005:
        stop = f"跌破 {stop_ref:.3f}（按至少 2x ATR 预留波动缓冲）或主线/催化失效时重新评估"
    else:
        stop = f"跌破 {stop_ref:.3f} 或主线/催化失效时重新评估"
    buy_range = (
        _format_execution_price_range(buy_low_ref, buy_high_ref)
        if buy_low_ref and buy_high_ref
        else buy_range_note or "暂不设，先等右侧确认"
    )
    trim_range = _format_execution_price_range(trim_low_ref, trim_high_ref)

    # --- Portfolio-level position management ---
    if risk_score >= 70:
        max_exposure = "单标的 ≤10%"
    elif risk_score >= 50:
        max_exposure = "单标的 ≤6%"
    else:
        max_exposure = "单标的 ≤3%"

    if rating >= 3:
        scaling = _formal_scaling_plan_from_setup(
            trade_state=trade_state,
            entry=entry,
            buy_range=buy_range,
            target=target,
            technical_score=int(tech or 0),
            catalyst_score=int(catalyst_score or 0),
            relative_score=int(relative_score or 0),
        )
    elif etf_trend_continuation:
        scaling = "分 2 批跟踪，回踩承接或放量确认后再考虑第二笔"
    elif rating == 2:
        scaling = "一次性小仓位，不加仓"
    else:
        scaling = _watch_scaling_plan_from_scores(
            technical_score=int(tech or 0),
            fundamental_score=int(fundamental_score or 0),
            catalyst_score=int(catalyst_score or 0),
            relative_score=int(relative_score or 0),
            risk_score=int(risk_score or 0),
        )

    corr_warning = ""
    if correlation_pair and len(correlation_pair) >= 2:
        corr_symbol, corr_value = correlation_pair[0], correlation_pair[1]
        if corr_value is not None and float(corr_value) > 0.7:
            corr_warning = f"与持仓 {corr_symbol} 相关度 {float(corr_value):.2f}，注意合计敞口"

    horizon = build_analysis_horizon_profile(
        rating=rating,
        asset_type=asset_type,
        technical_score=int(tech or 0),
        fundamental_score=int(fundamental_score or 0),
        catalyst_score=int(catalyst_score or 0),
        relative_score=int(relative_score or 0),
        risk_score=int(risk_score or 0),
        macro_reverse=bool(macro_reverse),
        trade_state=trade_state,
        direction=direction,
        position=position,
        price_percentile_1y=price_percentile_1y,
        rsi=rsi,
        false_break_kind=false_break_kind,
        divergence_signal=divergence_signal,
        near_pressure=pressure_award < 0,
        phase_label=phase_label,
    )

    return {
        "direction": direction,
        "entry": entry,
        "position": position,
        "stop": stop,
        "target": target,
        "stop_ref": stop_ref,
        "target_ref": target_ref,
        "buy_low_ref": buy_low_ref,
        "buy_high_ref": buy_high_ref,
        "buy_range": buy_range,
        "trim_low_ref": trim_low_ref,
        "trim_high_ref": trim_high_ref,
        "trim_range": trim_range,
        "target_pct": float(target_ref / close_now - 1) if close_now else 0.0,
        "timeframe": timeframe,
        "max_portfolio_exposure": max_exposure,
        "scaling_plan": scaling,
        "stop_loss_pct": stop_loss_pct,
        "correlated_warning": corr_warning,
        "horizon": horizon,
    }


def _retry_china_history_after_failure(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
) -> pd.DataFrame:
    if asset_type not in {"cn_stock", "cn_etf", "cn_index", "cn_fund"}:
        raise ValueError(f"Unsupported China asset type retry: {asset_type}")
    collector = ChinaMarketCollector(dict(config))
    context = get_asset_context(symbol, asset_type, dict(config))
    source_symbol = context.source_symbol
    if asset_type == "cn_stock":
        return collector.get_stock_daily(source_symbol)
    if asset_type == "cn_etf":
        return collector.get_etf_daily(source_symbol)
    if asset_type == "cn_index":
        return collector.get_index_daily(symbol, proxy_symbol=source_symbol)
    return collector.get_open_fund_daily(symbol, proxy_symbol=source_symbol)


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
    fund_profile = _collect_fund_profile(symbol, asset_type, config) if asset_type in {"cn_fund", "cn_etf"} else {}
    runtime_context["fund_profile"] = fund_profile
    if fund_profile:
        metadata = _enrich_metadata_with_fund_profile(metadata, fund_profile)
    industry_index_snapshot: Dict[str, Any] = {}
    if asset_type in {"cn_stock", "cn_etf"}:
        industry_index_snapshot = _context_industry_index_snapshot(metadata, runtime_context, fund_profile=fund_profile)
        if industry_index_snapshot:
            metadata = _enrich_metadata_with_industry_index_snapshot(metadata, industry_index_snapshot)
    if asset_type == "cn_stock":
        stock_theme_membership = _cn_stock_theme_membership_snapshot(metadata, runtime_context)
        if stock_theme_membership:
            metadata = _enrich_metadata_with_stock_theme_membership(metadata, stock_theme_membership)
    index_topic_bundle: Dict[str, Any] = {}
    if _asset_uses_index_topic_bundle(metadata, fund_profile=fund_profile, asset_type=asset_type):
        index_topic_bundle = _context_index_topic_bundle(metadata, runtime_context, fund_profile=fund_profile)
        if index_topic_bundle:
            metadata = _enrich_metadata_with_index_topic_bundle(metadata, index_topic_bundle)
    notes: List[str] = [
        str(item).strip()
        for item in (fund_profile.get("notes") or [])
        if str(item).strip() and any(token in str(item) for token in ("降级", "异常", "不可用"))
    ]
    history_fallback_mode = False
    try:
        raw_history = fetch_asset_history(symbol, asset_type, dict(config))
        history = normalize_ohlcv_frame(raw_history)
        history_source = str(getattr(raw_history, "attrs", {}).get("history_source", "") or getattr(history, "attrs", {}).get("history_source", "")).strip()
        history_source_label = str(getattr(raw_history, "attrs", {}).get("history_source_label", "") or getattr(history, "attrs", {}).get("history_source_label", "")).strip()
        if history_source:
            metadata = dict(metadata)
            metadata["history_source"] = history_source
        if history_source_label:
            metadata = dict(metadata)
            metadata["history_source_label"] = history_source_label
    except Exception as exc:
        retry_exc = exc
        retry_history = None
        if asset_type in {"cn_stock", "cn_etf", "cn_index", "cn_fund"}:
            try:
                retry_history = _retry_china_history_after_failure(symbol, asset_type, config)
            except Exception as second_exc:  # pragma: no cover - only hit on repeated vendor failure
                retry_exc = second_exc
        if retry_history is not None and not retry_history.empty:
            raw_history = retry_history
            history = normalize_ohlcv_frame(raw_history)
            history_source = str(getattr(raw_history, "attrs", {}).get("history_source", "") or getattr(history, "attrs", {}).get("history_source", "")).strip()
            history_source_label = str(getattr(raw_history, "attrs", {}).get("history_source_label", "") or getattr(history, "attrs", {}).get("history_source_label", "")).strip()
            if history_source:
                metadata = dict(metadata)
                metadata["history_source"] = history_source
            if history_source_label:
                metadata = dict(metadata)
                metadata["history_source_label"] = history_source_label
            notes.append("历史日线首轮抓取失败后已重试中国市场主链，并成功恢复完整日线。")
        else:
            fallback_history = build_snapshot_fallback_history(symbol, asset_type, config, periods=60)
            if fallback_history is None or fallback_history.empty:
                raise retry_exc
            history = normalize_ohlcv_frame(fallback_history)
            metadata = dict(metadata)
            metadata["history_fallback"] = True
            metadata["history_fallback_reason"] = str(retry_exc)
            metadata["history_source"] = "snapshot_fallback"
            metadata["history_source_label"] = "本地实时快照占位"
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
        "technical": _technical_dimension(
            history,
            technical,
            symbol=symbol,
            asset_type=asset_type,
            metadata=metadata,
            config=config,
        ),
        "fundamental": _fundamental_dimension(symbol, asset_type, metadata, metrics, config, fund_profile, runtime_context),
        "catalyst": _catalyst_dimension(metadata, runtime_context, fund_profile),
        "relative_strength": _relative_strength_dimension(symbol, asset_type, metadata, metrics, asset_returns, runtime_context),
        "chips": _chips_dimension(symbol, asset_type, metadata, runtime_context, config, history),
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
    rating = _rating_from_dimensions(dimensions, warnings, asset_type=asset_type, metadata=metadata)
    if exclusion_reasons:
        rating = _cap_rating_for_hard_exclusions(rating, exclusion_reasons)
    action = _action_plan(
        {"rating": rating, "dimensions": dimensions, "asset_type": asset_type, "metadata": metadata},
        history,
        technical,
        correlation_pair,
        metrics,
    )
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
        history,
        metrics,
        dimensions,
        technical,
        action,
        runtime_context,
        fund_profile,
    )
    theme_playbook = _analysis_theme_playbook_context(
        metadata,
        runtime_context,
        fund_profile=fund_profile,
        narrative=narrative,
        notes=notes,
    )
    if bool(dict(config).get("skip_analysis_proxy_signals_runtime", False)):
        proxy_signals = {}
    else:
        proxy_signals = _analysis_proxy_signals(
            symbol=symbol,
            metrics=metrics,
            technical=technical,
            runtime_context=runtime_context,
            config=config,
        )

    result = {
        "symbol": symbol,
        "name": str(metadata.get("name", symbol)),
        "asset_type": asset_type,
        "metadata": metadata,
        "fund_profile": fund_profile,
        "industry_index_snapshot": industry_index_snapshot,
        "index_topic_bundle": index_topic_bundle,
        "news_report": dict(runtime_context.get("news_report") or {}),
        "market_event_rows": _market_event_rows_from_context(metadata, runtime_context, fund_profile),
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
        "theme_playbook": theme_playbook,
        "proxy_signals": proxy_signals,
        "history_fallback_mode": history_fallback_mode,
        "excluded": bool(exclusion_reasons),
        "exclusion_reasons": exclusion_reasons,
        "correlation_pair": correlation_pair,
    }
    result["provenance"] = build_analysis_provenance(result)
    return result


def _signal_confidence_warning_line(confidence: Mapping[str, Any]) -> str:
    payload = dict(confidence or {})
    if not payload or not payload.get("available"):
        return ""
    stop_rate = payload.get("stop_hit_rate")
    target_rate = payload.get("target_hit_rate")
    try:
        stop_value = float(stop_rate)
    except (TypeError, ValueError):
        return ""
    try:
        target_value = float(target_rate) if target_rate is not None else None
    except (TypeError, ValueError):
        target_value = None
    if target_value is not None and stop_value > 0.5 and target_value < 0.2:
        return f"历史相似样本止损触发率 {stop_value:.0%}、目标触达率 {target_value:.0%}，当前执行容错率偏低。"
    if stop_value > 0.6:
        return f"历史相似样本止损触发率 {stop_value:.0%}，说明同类信号的止损频率偏高。"
    return ""


def _append_sentence(base: str, extra: str) -> str:
    head = str(base).strip()
    tail = str(extra).strip()
    if not tail:
        return head
    if not head:
        return tail
    if head.endswith(("。", "！", "？")):
        return f"{head}{tail}"
    return f"{head} {tail}"


def _format_execution_price_range(low: Optional[float], high: Optional[float]) -> str:
    if low is None or high is None:
        return ""
    lower = float(min(low, high))
    upper = float(max(low, high))
    if not math.isfinite(lower) or not math.isfinite(upper):
        return ""
    if upper - lower <= max(0.001, upper * 0.003):
        return f"{upper:.3f} 附近"
    return f"{lower:.3f} - {upper:.3f}"


def _prepend_context(base: str, context: str) -> str:
    text = str(base).strip()
    prefix = str(context).strip()
    if not text or not prefix:
        return text
    if text.startswith(prefix):
        return text
    return f"{prefix}{text}"


def _dimension_constraint_phrase(dimension: Mapping[str, Any]) -> str:
    for factor in list(dimension.get("factors") or []):
        signal = str(factor.get("signal", "")).strip()
        display = str(factor.get("display_score", "")).strip()
        if not signal or signal in {"—", "缺失", "不适用"}:
            continue
        if display.startswith("-") or display.startswith("0/"):
            return signal
    for key in ("core_signal", "summary"):
        text = str(dimension.get(key, "")).strip()
        if text and text not in {"当前没有明确亮点", "当前没有额外说明。"}:
            return text
    return ""


def _action_guidance_hint(analysis: Mapping[str, Any]) -> Dict[str, str]:
    dimensions = dict(analysis.get("dimensions") or {})
    ordered = [
        ("relative_strength", "相对强弱"),
        ("technical", "技术面"),
        ("catalyst", "催化面"),
        ("risk", "风险特征"),
        ("seasonality", "季节/日历"),
    ]
    candidates: List[tuple[int, str, str]] = []
    for key, label in ordered:
        dimension = dict(dimensions.get(key) or {})
        score = dimension.get("score")
        if score is None:
            continue
        phrase = _dimension_constraint_phrase(dimension)
        if not phrase:
            continue
        candidates.append((int(score), label, phrase.replace("；", "，")))
    if not candidates:
        return {}
    candidates.sort(key=lambda item: item[0])
    _, label, phrase = candidates[0]
    if len(phrase) > 36:
        phrase = phrase[:36].rstrip("，；。 ") + "..."
    if label == "催化面" and any(token in phrase for token in ("情报偏弱", "窗口暂不突出", "事件暂不突出", "情报覆盖偏窄", "情报偏少")):
        focus = "催化面还缺新增直接情报确认"
    elif label == "相对强弱" and any(token in phrase for token in ("跑输", "弱", "扩散", "没有明显下一棒")):
        focus = "相对强弱还没转强"
    elif label == "技术面" and any(token in phrase for token in ("未金叉", "未站回", "背离", "跌破", "偏弱")):
        focus = "技术面还缺右侧确认"
    elif label == "风险特征" and any(token in phrase for token in ("高点", "高估值", "高波动", "回撤", "分位")):
        focus = "风险收益比还不够舒服"
    elif label == "季节/日历" and any(token in phrase for token in ("窗口", "不在", "样本")):
        focus = "时间窗口还不占优"
    else:
        focus = f"{label}还停在“{phrase}”"
    return {
        "fit": f"眼下更卡在{focus}。",
        "misfit": f"在{focus}改善前，不要把观察仓误解成趋势已经重启。",
        "scaling": f"先等{focus}改善，再讨论第二笔。",
    }


def _personalize_action_guidance(analysis: Dict[str, Any]) -> None:
    action = dict(analysis.get("action") or {})
    if not action:
        return
    horizon = dict(action.get("horizon") or {})
    if horizon_family_code(horizon) != "watch":
        return
    hint = _action_guidance_hint(analysis)
    if not hint:
        return
    name = str(analysis.get("name", "")).strip()
    context_prefix = f"对{name}来说，" if name else ""
    if horizon:
        horizon["style"] = _prepend_context(str(horizon.get("style", "")), context_prefix)
        horizon["fit_reason"] = _prepend_context(str(horizon.get("fit_reason", "")), context_prefix)
        horizon["fit_reason"] = _append_sentence(str(horizon.get("fit_reason", "")), hint["fit"])
        horizon["misfit_reason"] = _prepend_context(str(horizon.get("misfit_reason", "")), context_prefix)
        horizon["misfit_reason"] = _append_sentence(str(horizon.get("misfit_reason", "")), hint["misfit"])
        action["horizon"] = horizon
    if action.get("scaling_plan"):
        action["scaling_plan"] = _prepend_context(str(action.get("scaling_plan", "")), context_prefix)
        action["scaling_plan"] = _append_sentence(str(action.get("scaling_plan", "")), hint["scaling"])
    analysis["action"] = action


def _watch_scaling_plan_from_scores(
    *,
    technical_score: int,
    fundamental_score: int,
    catalyst_score: int,
    relative_score: int,
    risk_score: int,
    stop_hit_rate: float | None = None,
    win_rate_20d: float | None = None,
    confidence_score: int | None = None,
) -> str:
    if stop_hit_rate is not None and stop_hit_rate >= 0.6:
        return "观察名单阶段，不预设加仓"
    if risk_score >= 70 and technical_score >= 35 and catalyst_score < 20:
        return "先按防守观察仓理解，等催化补齐后再讨论第二笔"
    if fundamental_score >= 70 and relative_score < 40:
        return "等轮动修复后，再重开加仓计划"
    if relative_score >= 60 and fundamental_score < 30:
        return "先盯基本面约束能否缓解，再决定是否给第二笔"
    if win_rate_20d is not None and win_rate_20d >= 0.65 and technical_score < 45:
        return "先等右侧确认，再决定是否开启第二笔"
    if catalyst_score >= 50 and technical_score < 40:
        return "先看催化能否转成趋势，再谈第二笔"
    if catalyst_score < 20 and technical_score >= 35:
        return "先等量能或催化补齐，再决定是否从观察转成试仓"
    if technical_score < 35:
        return "不抢反弹，不预设加仓"
    if fundamental_score < 25:
        return "先把基本面风险看清，再决定是否值得给第二笔"
    if confidence_score is not None and confidence_score < 40:
        return "样本置信度偏低，暂不预设加仓"
    return "先列观察名单，不预设加仓"


def _formal_scaling_plan_from_setup(
    *,
    trade_state: str,
    entry: str,
    buy_range: str,
    target: str,
    technical_score: int,
    catalyst_score: int,
    relative_score: int,
) -> str:
    entry_text = str(entry or "").strip()
    buy_range_text = str(buy_range or "").strip()
    target_text = str(target or "").strip()
    has_live_buy_range = bool(buy_range_text) and "暂不设" not in buy_range_text

    if "持有优于追高" in trade_state:
        line = (
            f"不追高，先按 `{buy_range_text}` 一带分 2 批承接，再看能否补第二笔。"
            if has_live_buy_range
            else "不追高，先等回踩承接后再考虑第二笔。"
        )
        if entry_text:
            line = _append_sentence(line, f"更适合围绕 `{entry_text}` 这类确认去做。")
        return line

    if "等右侧确认" in trade_state:
        if has_live_buy_range:
            line = f"先按 `{buy_range_text}` 一带小仓试，只有 `{entry_text or '右侧确认'}` 命中后再补第二笔。"
        else:
            line = f"先不急着摊平，等 `{entry_text or '右侧确认'}` 命中后再分 2 批推进。"
        if relative_score < 40:
            line = _append_sentence(line, "轮动承接还不算充足，第二笔更该等相对强弱继续确认。")
        return line

    line = (
        f"先按 `{buy_range_text}` 一带分 2-3 批承接，确认后再补第二笔。"
        if has_live_buy_range
        else "先分 2-3 批试仓，确认后再补第二笔。"
    )
    if entry_text:
        line = _append_sentence(line, f"当前最关键的确认还是 `{entry_text}`。")
    elif target_text:
        line = _append_sentence(line, f"上行先看 `{target_text}` 一带能否承压后继续突破。")
    if catalyst_score < 25:
        line = _append_sentence(line, "催化还在跟进阶段，不适合一次性把仓位打满。")
    elif technical_score < 35:
        line = _append_sentence(line, "技术结构还没完全顺滑，第二笔更适合等走势继续修复。")
    return line


def _refresh_action_from_signal_confidence(analysis: Dict[str, Any]) -> None:
    confidence = dict(analysis.get("signal_confidence") or {})
    action = dict(analysis.get("action") or {})
    if not action:
        return

    dimensions = dict(analysis.get("dimensions") or {})
    technical_score = int(dict(dimensions.get("technical") or {}).get("score") or 0)
    fundamental_score = int(dict(dimensions.get("fundamental") or {}).get("score") or 0)
    catalyst_score = int(dict(dimensions.get("catalyst") or {}).get("score") or 0)
    relative_score = int(dict(dimensions.get("relative_strength") or {}).get("score") or 0)
    risk_score = int(dict(dimensions.get("risk") or {}).get("score") or 0)
    technical_raw = dict(analysis.get("technical_raw") or {})
    metrics_payload = dict(analysis.get("metrics") or {})
    social_aggregate = dict(dict(dict(analysis.get("proxy_signals") or {}).get("social_sentiment") or {}).get("aggregate") or {})
    false_break_kind = str(dict(dict(technical_raw.get("setup") or {}).get("false_break") or {}).get("kind", "none"))
    divergence = dict(technical_raw.get("divergence") or {})
    divergence_signal = str(divergence.get("signal", "neutral"))
    history = analysis.get("history")
    if divergence_signal != "neutral" and history is not None:
        divergence_age_days = _divergence_signal_age_days(divergence, history["date"].iloc[-1])
        if divergence_age_days is not None and divergence_age_days > 7:
            divergence_signal = "neutral"
    pressure_award = int(_find_factor(dimensions.get("technical") or {}, "压力位").get("awarded", 0) or 0)
    phase_label = str(dict(dict(analysis.get("narrative") or {}).get("phase") or {}).get("label", "")).strip()
    if not phase_label:
        phase_label, _ = _phase_label(dimensions, technical_raw)
    action["horizon"] = build_analysis_horizon_profile(
        rating=int(dict(analysis.get("rating") or {}).get("rank", 0) or 0),
        asset_type=str(analysis.get("asset_type", "")),
        technical_score=technical_score,
        fundamental_score=fundamental_score,
        catalyst_score=catalyst_score,
        relative_score=relative_score,
        risk_score=risk_score,
        macro_reverse=bool(dict(dimensions.get("macro") or {}).get("macro_reverse", False)),
        trade_state=str(dict(dict(analysis.get("narrative") or {}).get("judgment") or {}).get("state", "")),
        direction=str(action.get("direction", "")),
        position=str(action.get("position", "")),
        stop_hit_rate=confidence.get("stop_hit_rate"),
        win_rate_20d=confidence.get("win_rate_20d"),
        confidence_score=int(confidence.get("confidence_score", 0) or 0) if confidence.get("confidence_score") is not None else None,
        price_percentile_1y=float(metrics_payload.get("price_percentile_1y", 0.5) or 0.5),
        rsi=float(dict(technical_raw.get("rsi") or {}).get("RSI", 50.0) or 50.0),
        sentiment_index=social_aggregate.get("sentiment_index"),
        false_break_kind=false_break_kind,
        divergence_signal=divergence_signal,
        near_pressure=pressure_award < 0,
        phase_label=phase_label,
    )

    if horizon_family_code(action["horizon"]) == "watch":
        try:
            stop_value = float(confidence.get("stop_hit_rate")) if confidence.get("stop_hit_rate") is not None else None
        except (TypeError, ValueError):
            stop_value = None
        try:
            win_value = float(confidence.get("win_rate_20d")) if confidence.get("win_rate_20d") is not None else None
        except (TypeError, ValueError):
            win_value = None
        try:
            confidence_value = int(confidence.get("confidence_score")) if confidence.get("confidence_score") is not None else None
        except (TypeError, ValueError):
            confidence_value = None
        action["scaling_plan"] = _watch_scaling_plan_from_scores(
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            stop_hit_rate=stop_value,
            win_rate_20d=win_value,
            confidence_score=confidence_value,
        )

    warning_line = _signal_confidence_warning_line(confidence)
    if warning_line:
        rating_payload = dict(analysis.get("rating") or {})
        warnings = [str(item).strip() for item in (rating_payload.get("warnings") or []) if str(item).strip()]
        if warning_line not in warnings:
            warnings.append(warning_line)
        rating_payload["warnings"] = warnings
        analysis["rating"] = rating_payload

        conclusion = str(analysis.get("conclusion", "")).strip()
        if warning_line not in conclusion:
            analysis["conclusion"] = f"{conclusion} {warning_line}".strip()

    analysis["action"] = action
    _personalize_action_guidance(analysis)


def _attach_signal_confidence(
    analyses: Sequence[Dict[str, Any]],
    config: Mapping[str, Any],
    *,
    limit: int = SIGNAL_CONFIDENCE_TOP_LIMIT,
) -> None:
    if not analyses:
        return
    technical_config = dict(config).get("technical", {})
    for analysis in list(analyses)[: max(int(limit), 0)]:
        history = analysis.get("history")
        if history is None:
            continue
        action = dict(analysis.get("action") or {})
        analysis["signal_confidence"] = build_signal_confidence(
            history,
            asset_type=str(analysis.get("asset_type", "")),
            technical_config=technical_config,
            stop_loss_pct=action.get("stop_loss_pct", "-8%"),
            target_pct=action.get("target_pct", 0.12),
            history_fallback=bool(analysis.get("history_fallback_mode")),
        )
        _refresh_action_from_signal_confidence(analysis)


def _fund_pool_composite_text(row: Mapping[str, Any]) -> str:
    return " ".join(
        [
            str(row.get("name", "") or "").strip(),
            str(row.get("benchmark", "") or "").strip(),
            str(row.get("fund_type", "") or "").strip(),
            str(row.get("invest_type", "") or "").strip(),
            str(row.get("management", "") or "").strip(),
        ]
    ).strip()


def _fund_theme_text(text: str) -> str:
    cleaned = str(text or "")
    for noise in (
        "中国人民银行人民币活期存款利率",
        "银行活期存款利率",
        "人民币活期存款利率",
        "活期存款利率",
        "税后",
    ):
        cleaned = cleaned.replace(noise, " ")
    return cleaned


def _fund_base_name(name: str) -> str:
    normalized = str(name or "").strip()
    normalized = re.sub(r"[\s（）()]+$", "", normalized)
    normalized = re.sub(r"(人民币|美元现汇|美元现钞|场内份额|场外份额)$", "", normalized)
    if re.search(r"联接[A-Z]$", normalized):
        normalized = normalized[:-1]
    elif len(normalized) >= 2 and normalized[-1].isalpha() and not normalized[-2].isascii():
        normalized = normalized[:-1]
    return normalized.strip() or str(name or "").strip()


def _fund_share_class_priority(name: str) -> int:
    normalized = str(name or "").strip().upper()
    if len(normalized) < 2 or normalized[-2].isascii():
        return 0
    for marker, score in (
        ("C", 4),
        ("A", 3),
        ("E", 2),
        ("I", 1),
        ("Y", 1),
        ("F", 1),
        ("B", 1),
    ):
        if normalized.endswith(marker):
            return score
    return 0


def _preferred_fund_sectors(day_theme: Any, theme_filter: str = "") -> List[str]:
    preferred: List[str] = []
    theme_texts = [theme_filter]
    if isinstance(day_theme, Mapping):
        theme_texts.extend(_day_theme_labels(day_theme))
    else:
        label = str(day_theme or "").strip()
        if label:
            theme_texts.append(label)
    for text in theme_texts:
        sector, _ = _normalize_sector(str(text or ""))
        if sector != "综合" and sector not in preferred:
            preferred.append(sector)
    lowered_theme = " ".join(str(text or "").lower() for text in theme_texts if str(text or "").strip())
    if any(token in lowered_theme for token in ("风险", "地缘", "防守", "避险")):
        for sector in ("黄金", "高股息", "宽基"):
            if sector not in preferred:
                preferred.append(sector)
    if "能源" in lowered_theme:
        for sector in ("能源", "黄金", "高股息"):
            if sector not in preferred:
                preferred.append(sector)
    if any(token in lowered_theme for token in ("科技", "ai", "算力", "半导体", "芯片")):
        for sector in ("科技", "半导体", "通信", "宽基"):
            if sector not in preferred:
                preferred.append(sector)
    if any(token in lowered_theme for token in ("通信", "光模块", "cpo", "数据中心", "运营商", "5g", "6g")):
        for sector in ("通信", "半导体", "科技", "宽基"):
            if sector not in preferred:
                preferred.append(sector)
    if any(token in lowered_theme for token in ("游戏", "传媒", "动漫", "aigc", "ai应用")):
        for sector in ("传媒", "科技", "宽基"):
            if sector not in preferred:
                preferred.append(sector)
    if any(token in lowered_theme for token in ("创新药", "医药", "bd", "license-out")):
        for sector in ("医药", "宽基"):
            if sector not in preferred:
                preferred.append(sector)
    return preferred


def _matches_fund_style_filter(row: Mapping[str, Any], style_filter: str) -> bool:
    style = str(style_filter or "").strip().lower()
    if style in {"", "all"}:
        return True
    normalized_management = str(row.get("_management_style", "") or row.get("management_style", "")).strip()
    normalized_scope = str(row.get("_exposure_scope", "") or row.get("exposure_scope", "")).strip()
    if style == "index" and normalized_management:
        return normalized_management in {"被动跟踪", "指数增强"}
    if style == "active" and normalized_management:
        return normalized_management == "主动管理"
    if style == "commodity" and normalized_scope:
        return normalized_scope == "商品"
    combined = " ".join(
        [
            str(row.get("fund_type", "") or "").strip(),
            str(row.get("invest_type", "") or "").strip(),
            str(row.get("benchmark", "") or "").strip(),
            str(row.get("name", "") or "").strip(),
        ]
    ).lower()
    if style == "index":
        return any(token in combined for token in ("指数", "被动", "增强指数"))
    if style == "active":
        return any(token in combined for token in ("混合", "灵活配置", "股票")) and not any(
            token in combined for token in ("指数", "被动", "增强指数", "黄金现货合约", "商品")
        )
    if style == "commodity":
        return any(token in combined for token in ("商品", "黄金", "贵金属", "现货合约"))
    return True


def _matches_manager_filter(row: Mapping[str, Any], manager_filter: str) -> bool:
    keyword = str(manager_filter or "").strip().lower()
    if not keyword:
        return True
    manager = str(row.get("management", "") or "").strip().lower()
    name = str(row.get("name", "") or "").strip().lower()
    return keyword in manager or keyword in name


def _sector_round_robin_head(
    frame: pd.DataFrame,
    *,
    sector_col: str,
    limit: int,
    preferred_sectors: Optional[Sequence[str]] = None,
) -> pd.DataFrame:
    """Keep sector breadth so one hot theme does not crowd out the whole pool.

    ``frame`` is expected to be sorted by the primary pre-rank already. This helper
    only changes which rows survive the candidate cap; it does not weaken any hard
    filters or re-score the rows.
    """
    if frame.empty or limit <= 0:
        return frame.head(0)
    if len(frame) <= limit:
        return frame

    sector_buckets: Dict[str, List[Any]] = {}
    seen_sectors: List[str] = []
    for index, raw_sector in zip(frame.index, frame[sector_col].tolist()):
        sector = str(raw_sector or "").strip() or "综合"
        if sector not in sector_buckets:
            sector_buckets[sector] = []
            seen_sectors.append(sector)
        sector_buckets[sector].append(index)

    preferred: List[str] = []
    for item in preferred_sectors or []:
        sector = str(item).strip()
        if sector and sector in sector_buckets and sector not in preferred:
            preferred.append(sector)

    ordered_sectors = [
        *preferred,
        *[sector for sector in seen_sectors if sector not in preferred and sector != "综合"],
    ]
    if "综合" in sector_buckets and "综合" not in ordered_sectors:
        ordered_sectors.append("综合")

    selected_indexes: List[Any] = []
    layer = 0
    while len(selected_indexes) < limit:
        progressed = False
        for sector in ordered_sectors:
            bucket = sector_buckets.get(sector) or []
            if layer >= len(bucket):
                continue
            selected_indexes.append(bucket[layer])
            progressed = True
            if len(selected_indexes) >= limit:
                break
        if not progressed:
            break
        layer += 1
    return frame.loc[selected_indexes]


def build_fund_pool(
    config: Mapping[str, Any],
    theme_filter: str = "",
    *,
    preferred_sectors: Optional[Sequence[str]] = None,
    max_candidates: Optional[int] = None,
    style_filter: str = "",
    manager_filter: str = "",
) -> tuple[List[PoolItem], List[str]]:
    warnings: List[str] = []
    watchlist = load_watchlist()
    pool: List[PoolItem] = []
    seen: set[str] = set()
    opportunity_cfg = dict(config).get("opportunity", {})
    max_scan_candidates = int(max_candidates or opportunity_cfg.get("fund_max_scan_candidates", 12))
    min_found_days = int(opportunity_cfg.get("fund_min_found_days", 120))
    lowered_filter = theme_filter.lower().strip()
    preferred = [str(item).strip() for item in (preferred_sectors or []) if str(item).strip()]

    try:
        frame = FundProfileCollector(config).get_fund_basic("O")
    except Exception as exc:
        return [], [f"Tushare 场外基金列表拉取失败: {exc}"]

    if frame is None or frame.empty:
        return [], ["Tushare 场外基金列表为空，无法构建全市场基金池。"]

    required_columns = {"ts_code", "name", "status"}
    missing_columns = [column for column in required_columns if column not in frame.columns]
    if missing_columns:
        return [], [f"Tushare 场外基金列表缺少必要列: {', '.join(missing_columns)}"]

    working = frame.copy()
    working = working[working["status"].fillna("").astype(str).eq("L")]
    if "delist_date" in working.columns:
        working = working[
            working["delist_date"].fillna("").astype(str).eq("")
            | working["delist_date"].fillna("").astype(str).str.lower().eq("nan")
        ]

    working["_symbol"] = working["ts_code"].fillna("").astype(str).str.split(".").str[0]
    working["_name"] = working["name"].fillna("").astype(str).str.strip()
    working = working[(working["_symbol"] != "") & (working["_name"] != "")]

    working["_found_date"] = pd.to_datetime(working.get("found_date"), format="%Y%m%d", errors="coerce")
    if min_found_days > 0:
        cutoff = pd.Timestamp(datetime.now()) - pd.Timedelta(days=min_found_days)
        working = working[working["_found_date"].isna() | (working["_found_date"] <= cutoff)]

    working["_composite_text"] = working.apply(lambda row: _fund_pool_composite_text(row).lower(), axis=1)
    excluded_pattern = "债券|货币|理财|fof|reits|reit|现金|短债|纯债|同业存单|政金债|国债|信用债|可转债"
    working = working[~working["_composite_text"].str.contains(excluded_pattern, na=False, regex=True)]

    taxonomy_payload = working.apply(
        lambda row: build_standard_fund_taxonomy(
            name=str(row.get("_name", "")),
            fund_type=str(row.get("fund_type", "") or ""),
            invest_type=str(row.get("invest_type", "") or ""),
            benchmark=str(row.get("benchmark", "") or ""),
            asset_type="cn_fund",
        ),
        axis=1,
    )
    working["_taxonomy"] = taxonomy_payload
    working["_management_style"] = taxonomy_payload.map(lambda item: str(item.get("management_style", "")))
    working["_exposure_scope"] = taxonomy_payload.map(lambda item: str(item.get("exposure_scope", "")))
    working["_taxonomy_labels"] = taxonomy_payload.map(lambda item: " ".join(str(label) for label in item.get("labels", [])))
    working["_taxonomy_terms"] = taxonomy_payload.map(lambda item: " ".join(_taxonomy_terms(item)))
    working["_filter_text"] = (
        working["_composite_text"]
        + " "
        + working["_taxonomy_labels"].fillna("").astype(str).str.lower()
        + " "
        + working["_taxonomy_terms"].fillna("").astype(str).str.lower()
        + " "
        + working["_management_style"].fillna("").astype(str).str.lower()
        + " "
        + working["_exposure_scope"].fillna("").astype(str).str.lower()
    )

    if lowered_filter:
        working = working[working["_filter_text"].str.contains(lowered_filter, na=False)]
    if str(style_filter).strip():
        style_mask = (
            working.apply(lambda row: _matches_fund_style_filter(row, style_filter), axis=1)
            if not working.empty
            else pd.Series(dtype=bool, index=working.index)
        )
        working = working[style_mask]
    if str(manager_filter).strip():
        manager_mask = (
            working.apply(lambda row: _matches_manager_filter(row, manager_filter), axis=1)
            if not working.empty
            else pd.Series(dtype=bool, index=working.index)
        )
        working = working[manager_mask]

    working["_sector"] = working["_taxonomy"].map(
        lambda item: item.get("sector", "综合") if isinstance(item, Mapping) else "综合"
    )
    working["_chain_nodes"] = working["_taxonomy"].map(
        lambda item: item.get("chain_nodes", list(DEFAULT_CHAIN_NODES)) if isinstance(item, Mapping) else list(DEFAULT_CHAIN_NODES)
    )
    working = working[working["_sector"] != "综合"]

    if working.empty:
        filters = []
        if lowered_filter:
            filters.append(f"主题={theme_filter}")
        if str(style_filter).strip():
            filters.append(f"风格={style_filter}")
        if str(manager_filter).strip():
            filters.append(f"管理人={manager_filter}")
        suffix = f"（筛选条件: {', '.join(filters)}）" if filters else ""
        return [], [f"当前全市场场外基金在初筛后没有留下可分析对象。{suffix}"]

    working["_base_name"] = working["_name"].map(_fund_base_name)
    working["_share_class_priority"] = working["_name"].map(_fund_share_class_priority)
    working["_issue_amount"] = pd.to_numeric(working.get("issue_amount"), errors="coerce").fillna(0.0)

    def _pre_rank(row: Mapping[str, Any]) -> float:
        score = 0.0
        sector = str(row.get("_sector", "")).strip()
        preferred_rank = _preferred_match_rank(
            preferred=preferred,
            sector=sector,
            taxonomy=dict(row.get("_taxonomy") or {}),
        )
        if preferred_rank >= 0:
            score += 80.0 - float(preferred_rank) * 5.0
        score += 10.0 if sector != "综合" else 0.0
        invest_type = str(row.get("invest_type", "")).strip()
        fund_type = str(row.get("fund_type", "")).strip()
        combined_type = f"{fund_type} {invest_type}"
        if any(token in combined_type for token in ("被动指数型", "增强指数型")):
            score += 12.0
        elif any(token in combined_type for token in ("股票型", "混合型", "灵活配置型", "商品型", "黄金现货合约")):
            score += 8.0
        score += min(float(row.get("_issue_amount", 0.0) or 0.0), 20.0)
        if pd.notna(row.get("_found_date")):
            days = max((pd.Timestamp(datetime.now()) - pd.Timestamp(row["_found_date"])).days, 0)
            if days >= 365:
                score += 8.0
            elif days >= 180:
                score += 4.0
        score += float(row.get("_share_class_priority", 0) or 0)
        if any(item.get("symbol") == row.get("_symbol") for item in watchlist):
            score += 3.0
        return score

    working["_pre_rank"] = working.apply(_pre_rank, axis=1)
    working = working.sort_values(
        by=["_base_name", "_pre_rank", "_share_class_priority", "_issue_amount"],
        ascending=[True, False, False, False],
    )
    working = working.drop_duplicates(subset=["_base_name"], keep="first")
    working = working.sort_values(by=["_pre_rank", "_issue_amount"], ascending=[False, False]).head(max_scan_candidates)

    for _, row in working.iterrows():
        symbol = str(row["_symbol"])
        if symbol in seen:
            continue
        taxonomy = dict(row.get("_taxonomy") or {}) if isinstance(row.get("_taxonomy"), Mapping) else {}
        metadata = {
            "benchmark": str(row.get("benchmark", "") or ""),
            "fund_type": str(row.get("fund_type", "") or ""),
            "invest_type": str(row.get("invest_type", "") or ""),
            "management": str(row.get("management", "") or ""),
            "found_date": "" if pd.isna(row.get("_found_date")) else str(pd.Timestamp(row["_found_date"]).date()),
            "issue_amount": float(row.get("_issue_amount", 0.0) or 0.0),
            "taxonomy": taxonomy,
        }
        metadata = _apply_theme_profile_metadata(metadata)
        pool.append(
            PoolItem(
                symbol=symbol,
                name=str(row["_name"]),
                asset_type="cn_fund",
                region="CN",
                sector=str(row["_sector"]),
                chain_nodes=list(row["_chain_nodes"]) if isinstance(row["_chain_nodes"], list) else list(DEFAULT_CHAIN_NODES),
                source="tushare_open_fund_basic",
                in_watchlist=any(item["symbol"] == symbol for item in watchlist),
                metadata=metadata,
            )
        )
        seen.add(symbol)

    return pool, warnings


def build_default_pool(
    config: Mapping[str, Any],
    theme_filter: str = "",
    *,
    preferred_sectors: Optional[Sequence[str]] = None,
) -> tuple[List[PoolItem], List[str]]:
    warnings: List[str] = []
    watchlist = load_watchlist()
    etf_watchlist = [item for item in watchlist if str(item.get("asset_type", "")).strip() == "cn_etf"]
    pool: List[PoolItem] = []
    seen: set[str] = set()
    opportunity_cfg = dict(config).get("opportunity", {})
    min_turnover = float(opportunity_cfg.get("min_turnover", 50_000_000))
    max_candidates = int(opportunity_cfg.get("max_scan_candidates", 30))
    lowered_filter = theme_filter.lower().strip()
    preferred = [str(item).strip() for item in (preferred_sectors or []) if str(item).strip()]

    def _extend_pool_from_frame(frame: pd.DataFrame, source: str) -> int:
        amount_col = "amount" if "amount" in frame.columns else None
        name_col = "name" if "name" in frame.columns else None
        symbol_col = "symbol" if "symbol" in frame.columns else None
        benchmark_col = "benchmark" if "benchmark" in frame.columns else None
        if not amount_col or not name_col or not symbol_col:
            return 0

        working = frame.copy()
        working[amount_col] = pd.to_numeric(working[amount_col], errors="coerce").fillna(0.0)
        working = working[working[amount_col] >= min_turnover]
        if working.empty:
            return 0

        if "list_date" in working.columns:
            latest_trade_date = str(working["trade_date"].iloc[0]).replace("-", "") if "trade_date" in working.columns and not working.empty else ""
            if latest_trade_date:
                working = working[
                    working["list_date"].fillna("").astype(str).str.replace("-", "").le(latest_trade_date)
                ]
        if "delist_date" in working.columns:
            working = working[
                working["delist_date"].fillna("").astype(str).eq("")
                | working["delist_date"].fillna("").astype(str).str.lower().eq("nan")
            ]

        is_etf_like = (
            working[name_col].astype(str).str.contains("ETF", case=False, na=False)
            | working.get("benchmark", pd.Series("", index=working.index)).fillna("").astype(str).str.contains("指数|收益率|商品|期货", na=False)
            | working.get("invest_type", pd.Series("", index=working.index)).fillna("").astype(str).str.contains("指数|被动|增强|QDII", na=False)
        )
        working = working[is_etf_like]
        if working.empty:
            return 0

        composite_text = working[name_col].astype(str)
        if benchmark_col:
            composite_text = composite_text + " " + working[benchmark_col].fillna("").astype(str)
        if "invest_type" in working.columns:
            composite_text = composite_text + " " + working["invest_type"].fillna("").astype(str)

        excluded_keywords = ("债", "货币", "国债", "政金", "现金", "利率", "短融", "同业存单", "信用债", "可转债")
        working = working[~composite_text.str.contains("|".join(excluded_keywords), na=False)]
        if working.empty:
            return 0

        taxonomy_payload = working.apply(
            lambda row: build_standard_fund_taxonomy(
                name=str(row.get(name_col, "")),
                fund_type=str(row.get("fund_type", "") or ""),
                invest_type=str(row.get("invest_type", "") or ""),
                benchmark=str(row.get("benchmark", "") or ""),
                asset_type="cn_etf",
            ),
            axis=1,
        )
        working["_taxonomy"] = taxonomy_payload
        working["_sector"] = taxonomy_payload.map(lambda item: str(item.get("sector", "综合")))
        working["_chain_nodes"] = taxonomy_payload.map(lambda item: list(item.get("chain_nodes") or []) or list(DEFAULT_CHAIN_NODES))
        working["_management_style"] = taxonomy_payload.map(lambda item: str(item.get("management_style", "")))
        working["_exposure_scope"] = taxonomy_payload.map(lambda item: str(item.get("exposure_scope", "")))
        working["_taxonomy_labels"] = taxonomy_payload.map(lambda item: " ".join(str(label) for label in item.get("labels", [])))
        working["_taxonomy_terms"] = taxonomy_payload.map(lambda item: " ".join(_taxonomy_terms(item)))
        working["_filter_text"] = (
            composite_text.fillna("").astype(str).str.lower()
            + " "
            + working["_taxonomy_labels"].fillna("").astype(str).str.lower()
            + " "
            + working["_taxonomy_terms"].fillna("").astype(str).str.lower()
            + " "
            + working["_management_style"].fillna("").astype(str).str.lower()
            + " "
            + working["_exposure_scope"].fillna("").astype(str).str.lower()
        )
        if lowered_filter:
            working = working[working["_filter_text"].str.contains(lowered_filter, na=False)]
        if working.empty:
            return 0

        working["_tracking_key"] = working.get("benchmark", pd.Series("", index=working.index)).fillna("").astype(str).str.strip().str.lower()
        working["_tracking_key"] = working["_tracking_key"].where(working["_tracking_key"] != "", working[name_col].fillna("").astype(str).str.strip().str.lower())
        working["_list_date"] = pd.to_datetime(working.get("list_date"), errors="coerce")
        working["_in_watchlist"] = working[symbol_col].astype(str).map(lambda symbol: any(item["symbol"] == symbol for item in etf_watchlist))

        def _pre_rank(row: Mapping[str, Any]) -> float:
            score = 0.0
            sector = str(row.get("_sector", "")).strip()
            preferred_rank = _preferred_match_rank(
                preferred=preferred,
                sector=sector,
                taxonomy=dict(row.get("_taxonomy") or {}),
            )
            if preferred_rank >= 0:
                score += 80.0 - float(preferred_rank) * 5.0
            score += 10.0 if sector != "综合" else 0.0
            management_style = str(row.get("_management_style", "")).strip()
            if management_style == "指数增强":
                score += 12.0
            elif management_style == "被动跟踪":
                score += 8.0
            exposure_scope = str(row.get("_exposure_scope", "")).strip()
            if exposure_scope in {"行业主题", "商品"}:
                score += 6.0
            elif exposure_scope == "宽基":
                score += 4.0
            score += min(float(row.get(amount_col, 0.0) or 0.0) / 100_000_000.0, 20.0)
            total_size = pd.to_numeric(pd.Series([row.get("total_size")]), errors="coerce").dropna()
            if not total_size.empty:
                score += min(float(total_size.iloc[0]) / 1_000_000.0, 10.0)
            if str(row.get("index_name", "") or row.get("benchmark", "")).strip():
                score += 4.0
            if str(row.get("list_status", "")).strip().upper() == "L":
                score += 2.0
            if pd.notna(row.get("_list_date")):
                listed_days = max((pd.Timestamp(datetime.now()) - pd.Timestamp(row["_list_date"])).days, 0)
                if listed_days >= 365:
                    score += 8.0
                elif listed_days >= 180:
                    score += 4.0
            if bool(row.get("_in_watchlist")):
                score += 3.0
            return score

        working["_pre_rank"] = working.apply(_pre_rank, axis=1)
        working = working.sort_values(by=["_tracking_key", "_pre_rank", amount_col], ascending=[True, False, False])
        working = working.drop_duplicates(subset=["_tracking_key"], keep="first")
        working = working.sort_values(by=["_pre_rank", amount_col], ascending=[False, False])
        working = _sector_round_robin_head(
            working,
            sector_col="_sector",
            limit=max_candidates,
            preferred_sectors=preferred,
        )
        added = 0
        for _, row in working.iterrows():
            symbol = str(row[symbol_col])
            if symbol in seen:
                continue
            name = str(row[name_col])
            taxonomy = dict(row.get("_taxonomy") or {})
            sector = str(row.get("_sector", taxonomy.get("sector", "综合")))
            chain_nodes = list(row.get("_chain_nodes") or taxonomy.get("chain_nodes") or []) or list(DEFAULT_CHAIN_NODES)
            metadata = {
                "benchmark": str(row.get("benchmark", "") or ""),
                "fund_type": str(row.get("fund_type", "") or ""),
                "invest_type": str(row.get("invest_type", "") or ""),
                "management": str(row.get("management", "") or ""),
                "index_code": str(row.get("index_code", "") or ""),
                "index_name": str(row.get("index_name", "") or ""),
                "exchange": str(row.get("exchange", "") or ""),
                "list_status": str(row.get("list_status", "") or ""),
                "etf_type": str(row.get("etf_type", "") or ""),
                "trade_date": str(row.get("trade_date", "") or ""),
                "share_as_of": str(row.get("trade_date_share", "") or row.get("trade_date", "") or ""),
                "total_share": float(row.get("total_share", 0.0) or 0.0) if pd.notna(row.get("total_share")) else None,
                "total_size": float(row.get("total_size", 0.0) or 0.0) if pd.notna(row.get("total_size")) else None,
                "taxonomy": taxonomy,
            }
            metadata = _apply_theme_profile_metadata(metadata)
            pool.append(
                PoolItem(
                    symbol=symbol,
                    name=name,
                    asset_type="cn_etf",
                    region="CN",
                    sector=sector,
                    chain_nodes=chain_nodes,
                    source=source,
                    turnover=float(row[amount_col]),
                    in_watchlist=bool(row.get("_in_watchlist")),
                    metadata=metadata,
                )
            )
            seen.add(symbol)
            added += 1
        return added

    tushare_universe_loaded = False
    realtime_universe_loaded = False
    market_collector = ChinaMarketCollector(config)
    try:
        universe = market_collector.get_etf_universe_snapshot()
        if universe is not None and not universe.empty:
            tushare_universe_loaded = _extend_pool_from_frame(universe, "tushare_etf_universe") > 0
    except Exception as exc:
        warnings.append(f"Tushare ETF 全市场快照拉取失败，已回退到 watchlist: {exc}")

    if not tushare_universe_loaded:
        try:
            realtime = market_collector.get_etf_realtime()
            basic = market_collector._ts_fund_basic_snapshot("E")
            if realtime is not None and not realtime.empty:
                realtime_frame = realtime.copy()
                realtime_frame["symbol"] = realtime_frame.get("代码", pd.Series("", index=realtime_frame.index)).astype(str).str.strip()
                realtime_frame["name"] = realtime_frame.get("名称", pd.Series("", index=realtime_frame.index)).astype(str).str.strip()
                realtime_frame["amount"] = pd.to_numeric(realtime_frame.get("成交额"), errors="coerce")
                if "数据日期" in realtime_frame.columns:
                    realtime_frame["trade_date"] = pd.to_datetime(realtime_frame["数据日期"], errors="coerce").dt.strftime("%Y-%m-%d")
                elif "更新时间" in realtime_frame.columns:
                    realtime_frame["trade_date"] = pd.to_datetime(realtime_frame["更新时间"], errors="coerce").dt.strftime("%Y-%m-%d")
                if basic is not None and not getattr(basic, "empty", False):
                    basic_frame = basic.copy()
                    basic_frame["symbol"] = basic_frame["ts_code"].astype(str).str.split(".").str[0]
                    realtime_frame = realtime_frame.merge(
                        basic_frame,
                        on="symbol",
                        how="left",
                        suffixes=("", "_basic"),
                    )
                realtime_universe_loaded = _extend_pool_from_frame(realtime_frame, "realtime_etf_universe") > 0
        except Exception as exc:
            warnings.append(f"ETF 实时全市场快照拉取失败，已回退到 watchlist: {exc}")

    if not tushare_universe_loaded and not realtime_universe_loaded:
        warnings.append("当前 ETF 扫描池回退到 watchlist；后续推荐可信度低于全市场快照模式。")

    fallback_watchlist = etf_watchlist if not tushare_universe_loaded and not realtime_universe_loaded else []
    for item in fallback_watchlist:
        if lowered_filter and lowered_filter not in str(item.get("name", "")).lower() and lowered_filter not in str(item.get("sector", "")).lower():
            continue
        if item["symbol"] in seen:
            continue
        metadata = _merge_metadata(str(item["symbol"]), str(item.get("asset_type", "cn_etf")), item, config)
        metadata["taxonomy"] = build_standard_fund_taxonomy(
            name=str(metadata.get("name", item["symbol"])),
            fund_type=str(metadata.get("fund_type", "") or ""),
            invest_type=str(metadata.get("invest_type", "") or ""),
            benchmark=str(metadata.get("benchmark", "") or ""),
            asset_type=str(item.get("asset_type", "cn_etf")),
            sector_hint=str(metadata.get("sector", "")),
        )
        metadata = _apply_theme_profile_metadata(metadata)
        taxonomy = dict(metadata.get("taxonomy") or {})
        pool.append(
            PoolItem(
                symbol=str(item["symbol"]),
                name=str(metadata.get("name", item["symbol"])),
                asset_type=str(item.get("asset_type", "cn_etf")),
                region=str(metadata.get("region", "CN")),
                sector=str(metadata.get("sector", taxonomy.get("sector", "综合"))),
                chain_nodes=list(metadata.get("chain_nodes") or taxonomy.get("chain_nodes") or DEFAULT_CHAIN_NODES),
                source="watchlist",
                in_watchlist=True,
                metadata=metadata,
            )
        )
        seen.add(item["symbol"])

    return pool, warnings


def _pick_coverage_state(analyses: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = list(analyses or [])
    if not rows:
        return {
            "news_mode": "unknown",
            "degraded": False,
            "structured_rate": 0.0,
            "direct_news_rate": 0.0,
            "total": 0,
            "summary": "当前没有可统计的样本。",
        }
    modes = [
        str(dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {}).get("news_mode", "unknown"))
        for item in rows
    ]
    news_mode = "live" if modes and all(mode == "live" for mode in modes) else ("proxy" if "proxy" in modes else (modes[0] if modes else "unknown"))
    structured_count = 0
    direct_count = 0
    degraded_count = 0
    for item in rows:
        coverage = dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {})
        if coverage.get("structured_event") or coverage.get("forward_event"):
            structured_count += 1
        if coverage.get("high_confidence_company_news"):
            direct_count += 1
        if coverage.get("degraded"):
            degraded_count += 1
    total = len(rows)
    return {
        "news_mode": news_mode,
        "degraded": news_mode != "live" or degraded_count > 0,
        "structured_rate": structured_count / total if total else 0.0,
        "direct_news_rate": direct_count / total if total else 0.0,
        "total": total,
        "summary": f"结构化事件覆盖 {structured_count}/{total}，高置信直接新闻覆盖 {direct_count}/{total}。",
    }


def _discover_mode_label(mode: str) -> str:
    return {
        "tushare_universe": "Tushare 全市场 ETF 快照",
        "realtime_universe": "实时 ETF 全市场快照",
        "watchlist_fallback": "watchlist 回退池",
        "mixed_pool": "全市场 + watchlist 混合池",
    }.get(str(mode), str(mode) or "未标注")


def _discover_source_label(source: str) -> str:
    return {
        "tushare_etf_universe": "Tushare 全市场 ETF 快照",
        "realtime_etf_universe": "实时 ETF 全市场快照",
        "watchlist": "watchlist 回退池",
    }.get(str(source), str(source) or "未标注")


def _discover_driver_type(
    analysis: Mapping[str, Any],
    *,
    theme_filter: str = "",
    preferred_sectors: Sequence[str] | None = None,
) -> tuple[str, str]:
    metadata = dict(analysis.get("metadata") or {})
    dimensions = dict(analysis.get("dimensions") or {})
    day_theme = str(dict(analysis.get("day_theme") or {}).get("label", "")).strip()
    subject_theme = subject_theme_label(analysis)
    sector = str(metadata.get("sector", "")).strip()
    sector_text = " ".join(
        [
            subject_theme,
            sector,
            str(analysis.get("name", "")),
            *subject_theme_terms(analysis, allow_day_theme=False),
        ]
    ).lower()
    preferred = [str(item).strip() for item in (preferred_sectors or []) if str(item).strip()]
    tech = int(dict(dimensions.get("technical") or {}).get("score") or 0)
    catalyst = int(dict(dimensions.get("catalyst") or {}).get("score") or 0)
    relative = int(dict(dimensions.get("relative_strength") or {}).get("score") or 0)
    risk = int(dict(dimensions.get("risk") or {}).get("score") or 0)
    macro = int(dict(dimensions.get("macro") or {}).get("score") or 0)

    defensive_match = any(token in sector_text for token in ("黄金", "红利", "高股息", "避险", "防守"))
    theme_match = bool(theme_filter and theme_filter.lower() in sector_text)
    if not theme_match and not theme_filter:
        theme_match = _theme_alignment(metadata, dict(analysis.get("day_theme") or {}))
    if not theme_match and not theme_filter and subject_theme:
        theme_match = any(token and token.lower() in sector_text for token in preferred)

    if defensive_match and risk >= 65:
        return "防守驱动", "今天更像用它承接防守或避险需求，风险特征分和产品属性比短线趋势更重要。"
    if theme_match and (catalyst >= 45 or macro >= 20 or relative >= 45):
        focus_theme = subject_theme or sector or day_theme or "未识别方向"
        if day_theme and focus_theme and focus_theme != day_theme:
            return "主线驱动", f"它的直接映射更靠近 `{focus_theme}`，也更符合 `{day_theme}` 这层盘面背景，因此优先进入预筛。"
        return "主线驱动", f"它和当前主线 `{focus_theme}` 更贴近，因此优先进入预筛。"
    if catalyst >= 60 and catalyst >= tech + 10:
        return "催化驱动", "短期进入视野主要因为事件/政策/新闻催化先亮灯，而不是价格已经完全确认。"
    if tech >= 60 and relative >= 55:
        return "趋势驱动", "价格结构和相对强弱更占优，说明它是从走势确认里冒出来的。"
    if risk >= 70:
        return "防守驱动", "进池更依赖回撤和防守属性，而不是趋势或催化全面共振。"
    if catalyst >= tech:
        return "催化驱动", "当前被发现更多是因为事件驱动比价格结构先走出来。"
    return "趋势驱动", "当前被发现更多是因为走势没有完全破坏，具备继续跟踪的结构基础。"


def _discover_horizon_label(analysis: Mapping[str, Any]) -> str:
    action = dict(analysis.get("action") or {})
    horizon = dict(action.get("horizon") or {})
    label = str(horizon.get("label", "")).strip()
    if "长线配置" in label:
        return "长线"
    if "中线配置" in label:
        return "中线"
    if "波段跟踪" in label:
        return "波段"
    if "短线交易" in label:
        return "短线"
    timeframe = str(action.get("timeframe", "")).strip()
    if "中线配置" in timeframe:
        return "中线"
    if "短线交易" in timeframe:
        return "短线"
    return "观察期"


def _discover_ready_for_next_step(analysis: Mapping[str, Any]) -> bool:
    rating = int(dict(analysis.get("rating") or {}).get("rank", 0) or 0)
    asset_type = str(analysis.get("asset_type", "")).strip()
    metadata = dict(analysis.get("metadata") or {})
    dimensions = dict(analysis.get("dimensions") or {})
    tech = int(dict(dimensions.get("technical") or {}).get("score") or 0)
    catalyst = int(dict(dimensions.get("catalyst") or {}).get("score") or 0)
    relative = int(dict(dimensions.get("relative_strength") or {}).get("score") or 0)
    risk = int(dict(dimensions.get("risk") or {}).get("score") or 0)
    if _fund_like_theme_confirmation_gate(asset_type, metadata, dimensions).get("applies"):
        return False
    if _stock_signal_consistency_gate(asset_type, dimensions).get("applies"):
        return False
    return rating >= 2 or (rating >= 1 and ((catalyst >= 60 and tech >= 40) or (risk >= 75 and relative >= 45)))


def _discover_next_step_commands(
    analysis: Mapping[str, Any],
    *,
    theme_filter: str = "",
) -> List[Dict[str, str]]:
    symbol = str(analysis.get("symbol", "")).strip()
    metadata = dict(analysis.get("metadata") or {})
    sector = str(metadata.get("sector", "")).strip()
    ready = _discover_ready_for_next_step(analysis)
    pick_theme = str(theme_filter or subject_theme_label(analysis) or sector).strip()
    steps: List[Dict[str, str]] = []
    if ready and symbol:
        steps.append(
            {
                "label": "deep_scan",
                "command": f"python -m src.commands.scan {symbol}",
                "reason": "先把单标的八维细节、验证点和执行计划展开，确认这不是只靠一个分数冒出来的假阳性。",
            }
        )
        if pick_theme:
            steps.append(
                {
                    "label": "etf_pick",
                    "command": f"python -m src.commands.etf_pick {pick_theme}",
                    "reason": "把它放回同主题 ETF 池里做正式排序，确认是否足够进入推荐链路。",
                }
            )
            steps.append(
                {
                    "label": "fund_pick",
                    "command": f"python -m src.commands.fund_pick --theme {pick_theme}",
                    "reason": "如果你想把同主题场外基金一起纳入候选，就切到 fund pick 做同主题预筛。",
                }
            )
        return steps

    steps.append(
        {
            "label": "continue_observe",
            "command": "继续观察",
            "reason": "当前更适合先盯验证点和主线延续，不建议直接跳进正式推荐或大仓位决策。",
        }
    )
    if symbol:
        steps.append(
            {
                "label": "deep_scan_later",
                "command": f"python -m src.commands.scan {symbol}",
                "reason": "如果后续技术面或催化重新增强，再做单标的深扫会更有效率。",
            }
        )
    return steps


def _discover_candidate_blockers(analysis: Mapping[str, Any]) -> List[str]:
    rating = dict(analysis.get("rating") or {})
    narrative = dict(analysis.get("narrative") or {})
    action = dict(analysis.get("action") or {})
    blockers: List[str] = [
        "discover 当前只是 pre-screen 入口，还没经过 ETF pick 的同池排序、回看和发布门禁。"
    ]
    meaning = str(rating.get("meaning", "")).strip()
    if meaning:
        blockers.append(meaning)
    blockers.extend(str(item).strip() for item in (narrative.get("cautions") or []) if str(item).strip())
    blockers.extend(str(item).strip() for item in (rating.get("warnings") or []) if str(item).strip())
    if str(action.get("direction", "")).strip() in {"观望", "观望偏多", "回避"}:
        blockers.append(f"当前动作仍偏 `{action.get('direction', '观望')}`，说明还没到正式推荐的执行阶段。")
    deduped: List[str] = []
    seen = set()
    for item in blockers:
        if item and item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped[:4]


def _discover_next_step_reason(analysis: Mapping[str, Any], driver_type: str) -> str:
    rating = dict(analysis.get("rating") or {})
    dimensions = dict(analysis.get("dimensions") or {})
    tech = int(dict(dimensions.get("technical") or {}).get("score") or 0)
    catalyst = int(dict(dimensions.get("catalyst") or {}).get("score") or 0)
    relative = int(dict(dimensions.get("relative_strength") or {}).get("score") or 0)
    risk = int(dict(dimensions.get("risk") or {}).get("score") or 0)
    if _discover_ready_for_next_step(analysis):
        if driver_type == "防守驱动":
            return f"它已经不是泛观察主题：风险特征 `{risk}` 分，且评级达到 `{rating.get('label', '已命中')}`，适合进入下一步复核。"
        if driver_type == "催化驱动":
            return f"催化面 `{catalyst}` 分已经足够亮，值得继续做 `scan`/同池 pick，确认催化是否能转成趋势。"
        if driver_type == "趋势驱动":
            return f"技术面 `{tech}` 分、相对强弱 `{relative}` 分，说明不只是故事存在，价格也开始配合。"
        return f"评级 `{rating.get('label', '已命中')}`，且主线/催化/价格至少有两项共振，已经够资格进入下一步候选。"
    return "它现在更像观察发现：有一条线索在亮，但还不足以直接进入正式 pick。"


def _discover_today_reason_lines(
    analysis: Mapping[str, Any],
    *,
    driver_type: str,
    driver_reason: str,
) -> List[str]:
    metadata = dict(analysis.get("metadata") or {})
    dimensions = dict(analysis.get("dimensions") or {})
    narrative = dict(analysis.get("narrative") or {})
    day_theme = str(dict(analysis.get("day_theme") or {}).get("label", "")).strip() or "未识别主线"
    subject_theme = subject_theme_label(analysis)
    sector = str(metadata.get("sector", "综合")).strip()
    focus_theme = subject_theme or sector or "综合"
    catalyst_summary = str(dict(dimensions.get("catalyst") or {}).get("summary", "")).strip()
    technical_summary = str(dict(dimensions.get("technical") or {}).get("summary", "")).strip()
    relative_summary = str(dict(dimensions.get("relative_strength") or {}).get("summary", "")).strip()
    risk_summary = str(dict(dimensions.get("risk") or {}).get("summary", "")).strip()

    lines = [
        (
            f"今天把它捞出来，首先是因为 `{focus_theme}` 这条线在 `{day_theme}` 背景下仍有观察价值。"
            if focus_theme != day_theme
            else f"今天把它捞出来，首先是因为 `{focus_theme}` 仍在当前盘面主线里。"
        ),
        f"`{driver_type}`：{driver_reason}",
    ]
    if driver_type == "防守驱动" and risk_summary:
        lines.append(f"当前更重要的是 `{risk_summary}`。")
    elif driver_type == "催化驱动" and catalyst_summary:
        lines.append(f"短线触发点主要来自 `{catalyst_summary}`。")
    elif driver_type == "趋势驱动" and technical_summary:
        lines.append(f"今天被发现更偏价格/趋势确认，核心是 `{technical_summary}`。")
    else:
        detail = relative_summary or technical_summary or catalyst_summary or risk_summary
        if detail:
            lines.append(f"辅助证据是 `{detail}`。")

    phase = str(dict(narrative.get("phase") or {}).get("label", "")).strip()
    if phase:
        lines.append(f"当前阶段更接近 `{phase}`，所以发现不等于立刻推荐。")

    deduped: List[str] = []
    seen = set()
    for item in lines:
        if item and item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped[:4]


def _discover_data_notes(analysis: Mapping[str, Any]) -> List[str]:
    notes: List[str] = []
    catalyst_coverage = dict(dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {}).get("coverage") or {})
    if catalyst_coverage.get("degraded"):
        notes.append("催化面存在降级，当前更多依赖结构化事件、主题映射或代理信号，不等于完整实时新闻覆盖。")
    news_mode = str(catalyst_coverage.get("news_mode", "")).strip()
    if news_mode == "proxy":
        notes.append("情报模式当前是 `proxy`，事件与主线判断要保留一层不确定性。")
    if bool(analysis.get("history_fallback_mode")):
        notes.append("完整日线历史当前不可用，趋势和风险判断只作参考。")
    notes.extend(
        str(item).strip()
        for item in (analysis.get("notes") or [])
        if str(item).strip() and any(token in str(item) for token in ("降级", "不可用", "快照", "盘中", "watchlist", "代理"))
    )
    deduped: List[str] = []
    seen = set()
    for item in notes:
        if item and item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped[:3]


def _discover_brief_candidate(
    analysis: Mapping[str, Any],
    *,
    theme_filter: str = "",
    preferred_sectors: Sequence[str] | None = None,
) -> Dict[str, Any]:
    enriched = dict(analysis)
    driver_type, driver_reason = _discover_driver_type(
        analysis,
        theme_filter=theme_filter,
        preferred_sectors=preferred_sectors,
    )
    enriched["discovery"] = {
        "bucket": "next_step" if _discover_ready_for_next_step(analysis) else "observe",
        "driver_type": driver_type,
        "driver_reason": driver_reason,
        "horizon_label": _discover_horizon_label(analysis),
        "today_reason_lines": _discover_today_reason_lines(analysis, driver_type=driver_type, driver_reason=driver_reason),
        "next_step_reason": _discover_next_step_reason(analysis, driver_type),
        "blockers": _discover_candidate_blockers(analysis),
        "next_steps": _discover_next_step_commands(analysis, theme_filter=theme_filter),
        "data_notes": _discover_data_notes(analysis),
    }
    return enriched


def _discover_observation_candidates(
    analyses: Sequence[Mapping[str, Any]],
    ready_symbols: Sequence[str],
    *,
    theme_filter: str = "",
    preferred_sectors: Sequence[str] | None = None,
    limit: int = 3,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    ready_symbol_set = {str(item) for item in ready_symbols if str(item).strip()}
    for item in analyses:
        symbol = str(item.get("symbol", "")).strip()
        if not symbol or symbol in ready_symbol_set:
            continue
        dimensions = dict(item.get("dimensions") or {})
        if int(dict(item.get("rating") or {}).get("rank", 0) or 0) <= 0 and not (
            int(dict(dimensions.get("fundamental") or {}).get("score") or 0) >= 60
            or int(dict(dimensions.get("catalyst") or {}).get("score") or 0) >= 50
            or int(dict(dimensions.get("risk") or {}).get("score") or 0) >= 70
            or (
                str(item.get("asset_type", "")).strip() in {"cn_etf", "cn_fund", "cn_index"}
                and int(dict(dimensions.get("relative_strength") or {}).get("score") or 0) >= 45
                and int(dict(dimensions.get("technical") or {}).get("score") or 0) >= 30
                and (
                    int(dict(dimensions.get("catalyst") or {}).get("score") or 0) >= 18
                    or int(dict(dimensions.get("fundamental") or {}).get("score") or 0) >= 25
                )
            )
        ):
            continue
        rows.append(
            _discover_brief_candidate(
                item,
                theme_filter=theme_filter,
                preferred_sectors=preferred_sectors,
            )
        )
        if len(rows) >= limit:
            break
    return rows


def _discover_pool_summary(
    pool: Sequence[PoolItem],
    *,
    passed: int,
    ready_count: int,
    observe_count: int,
    preferred_sectors: Sequence[str] | None = None,
    theme_filter: str = "",
    discovery_mode: str = "",
) -> Dict[str, Any]:
    source_counts: Dict[str, int] = {}
    sector_counts: Dict[str, int] = {}
    watchlist_count = 0
    for item in pool:
        source_counts[str(item.source)] = source_counts.get(str(item.source), 0) + 1
        sector = str(item.sector or "综合").strip()
        sector_counts[sector] = sector_counts.get(sector, 0) + 1
        if bool(item.in_watchlist):
            watchlist_count += 1

    ordered_sectors = sorted(sector_counts.items(), key=lambda pair: (-pair[1], pair[0]))
    preferred = [str(item).strip() for item in (preferred_sectors or []) if str(item).strip()]
    filter_rules = [
        "当前 discover 只做 ETF / 商品类场内基金预筛，不覆盖个股和场外基金正式推荐。",
        "池构建阶段会先排掉债券/货币/REIT/低成交额产品，并对同跟踪方向做去重。",
    ]
    if theme_filter:
        filter_rules.append(f"本轮额外应用主题过滤 `{theme_filter}`，未命中该主题语义的方向不进入本轮分析。")
    else:
        filter_rules.append("本轮没有手动主题过滤，优先按今日主线和 ETF taxonomy 做预筛排序。")
    if preferred:
        filter_rules.append(f"今日优先方向：{', '.join(preferred[:4])}。")

    return {
        "boundary_note": "当前 discover 只是 pre-pick 入口，用来给 `scan` / `etf_pick` / `fund_pick` 输送候选，而不是直接给正式推荐。",
        "scan_scope_note": "当前池只来自 ETF 全市场快照或 watchlist 回退，不是全资产发现器。",
        "mode_label": _discover_mode_label(discovery_mode),
        "source_rows": [[_discover_source_label(key), str(value)] for key, value in sorted(source_counts.items())],
        "sector_rows": [[sector, str(count)] for sector, count in ordered_sectors[:6]],
        "watchlist_hits": watchlist_count,
        "filter_rules": filter_rules,
        "summary_lines": [
            f"本轮最终进入分析 `{len(pool)}` 只 ETF，过硬排除后剩 `{passed}` 只。",
            f"其中达到“进入下一步 pick / deep scan”门槛的有 `{ready_count}` 只，仅适合继续观察的有 `{observe_count}` 只。",
            f"扫描模式：`{_discover_mode_label(discovery_mode)}`；watchlist 命中 `{watchlist_count}` 只。",
        ],
    }


def _discover_analysis_sort_key(item: Mapping[str, Any]) -> tuple[float, float, float, float, float]:
    dimensions = dict(item.get("dimensions") or {})
    return (
        float(int(dict(item.get("rating") or {}).get("rank", 0) or 0)),
        float(dict(dimensions.get("technical") or {}).get("score") or 0),
        float(dict(dimensions.get("relative_strength") or {}).get("score") or 0),
        float(dict(dimensions.get("catalyst") or {}).get("score") or 0),
        float(dict(dimensions.get("fundamental") or {}).get("score") or 0),
    )


def discover_opportunities(config: Mapping[str, Any], top_n: int = 5, theme_filter: str = "") -> Dict[str, Any]:
    context = build_market_context(config, relevant_asset_types=["cn_etf", "futures"])
    preferred_sectors = _preferred_fund_sectors(context.get("day_theme", {}), theme_filter)
    pool, pool_warnings = build_default_pool(config, theme_filter, preferred_sectors=preferred_sectors)
    passed = 0
    coverage_analyses: List[Dict[str, Any]] = []
    analyses: List[Dict[str, Any]] = []
    blind_spots: List[str] = list(pool_warnings)
    analysis_workers = max(1, min(int(dict(dict(config).get("opportunity") or {}).get("analysis_workers", 4) or 4), len(pool) or 1, 6))
    base_context = dict(context)
    if analysis_workers > 1 and len(pool) > 1:
        with ThreadPoolExecutor(max_workers=analysis_workers) as executor:
            future_map = {
                executor.submit(
                    analyze_opportunity,
                    item.symbol,
                    item.asset_type,
                    config,
                    context={**base_context, "runtime_caches": {}},
                    metadata_override={
                        "name": item.name,
                        "sector": item.sector,
                        "chain_nodes": item.chain_nodes,
                        "region": item.region,
                        "in_watchlist": item.in_watchlist,
                    },
                ): item
                for item in pool
            }
            for future in as_completed(future_map):
                item = future_map[future]
                try:
                    analysis = future.result()
                except Exception as exc:
                    blind_spots.append(_client_safe_issue(f"{item.symbol} 扫描失败", exc))
                    continue
                if analysis["excluded"]:
                    continue
                passed += 1
                coverage_analyses.append(analysis)
                if _discover_ready_for_next_step(analysis):
                    analyses.append(analysis)
    else:
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
                blind_spots.append(_client_safe_issue(f"{item.symbol} 扫描失败", exc))
                continue
            if analysis["excluded"]:
                continue
            passed += 1
            coverage_analyses.append(analysis)
            if _discover_ready_for_next_step(analysis):
                analyses.append(analysis)
    analyses.sort(key=_discover_analysis_sort_key, reverse=True)
    coverage_analyses.sort(key=_discover_analysis_sort_key, reverse=True)
    coverage = _pick_coverage_state(coverage_analyses)
    sources = {str(item.source) for item in pool}
    if sources == {"watchlist"}:
        discovery_mode = "watchlist_fallback"
    elif sources == {"tushare_etf_universe"}:
        discovery_mode = "tushare_universe"
    elif sources == {"realtime_etf_universe"}:
        discovery_mode = "realtime_universe"
    elif "watchlist" in sources:
        discovery_mode = "mixed_pool"
    else:
        discovery_mode = "realtime_universe" if "realtime_etf_universe" in sources else "tushare_universe"
    ready_candidates = [
        _discover_brief_candidate(item, theme_filter=theme_filter, preferred_sectors=preferred_sectors)
        for item in analyses[:top_n]
    ][:top_n]
    observation_candidates = _discover_observation_candidates(
        coverage_analyses,
        [str(item.get("symbol", "")) for item in ready_candidates],
        theme_filter=theme_filter,
        preferred_sectors=preferred_sectors,
        limit=max(3, min(top_n, 5)),
    )
    pool_summary = _discover_pool_summary(
        pool,
        passed=passed,
        ready_count=len([item for item in ready_candidates if dict(item.get("discovery") or {}).get("bucket") == "next_step"]),
        observe_count=len(observation_candidates),
        preferred_sectors=preferred_sectors,
        theme_filter=theme_filter,
        discovery_mode=discovery_mode,
    )
    coverage = dict(coverage)
    coverage_note = "当前催化/事件覆盖可直接作为 pre-screen 参考。" if not coverage.get("degraded") else "当前催化/事件覆盖存在降级，discovery 更适合作为发现线索，而不是直接推荐。"
    coverage["note"] = coverage_note
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_pool": len(pool),
        "passed_pool": passed,
        "runtime_context": context,
        "regime": context.get("regime", {}),
        "day_theme": context.get("day_theme", {}),
        "data_coverage": coverage,
        "coverage_analyses": coverage_analyses,
        "top": analyses[:top_n],
        "market_proxy": dict(context.get("global_flow") or {}),
        "proxy_contract": summarize_proxy_contracts_from_analyses(
            coverage_analyses,
            market_proxy=context.get("global_flow"),
        ),
        "ready_candidates": ready_candidates,
        "observation_candidates": observation_candidates,
        "blind_spots": blind_spots[:8],
        "theme_filter": theme_filter,
        "discovery_mode": discovery_mode,
        "preferred_sectors": preferred_sectors,
        "pool_summary": pool_summary,
    }


def discover_fund_opportunities(
    config: Mapping[str, Any],
    top_n: int = 8,
    theme_filter: str = "",
    *,
    max_candidates: Optional[int] = None,
    style_filter: str = "",
    manager_filter: str = "",
) -> Dict[str, Any]:
    context = build_market_context(config, relevant_asset_types=["cn_fund", "cn_etf", "futures"])
    preferred_sectors = _preferred_fund_sectors(context.get("day_theme", {}), theme_filter)
    pool_kwargs: Dict[str, Any] = {
        "theme_filter": theme_filter,
        "preferred_sectors": preferred_sectors,
        "max_candidates": max_candidates,
    }
    if style_filter.strip():
        pool_kwargs["style_filter"] = style_filter
    if manager_filter.strip():
        pool_kwargs["manager_filter"] = manager_filter
    pool, pool_warnings = build_fund_pool(
        config,
        **pool_kwargs,
    )
    passed = 0
    analyses: List[Dict[str, Any]] = []
    blind_spots: List[str] = list(pool_warnings)
    analysis_workers = max(1, min(int(dict(dict(config).get("opportunity") or {}).get("analysis_workers", 4) or 4), len(pool) or 1, 6))
    base_context = dict(context)
    if analysis_workers > 1 and len(pool) > 1:
        with ThreadPoolExecutor(max_workers=analysis_workers) as executor:
            future_map = {}
            for item in pool:
                override: Dict[str, Any] = {
                    "name": item.name,
                    "sector": item.sector,
                    "chain_nodes": item.chain_nodes,
                    "region": item.region,
                    "in_watchlist": item.in_watchlist,
                }
                if item.metadata:
                    override.update(item.metadata)
                future = executor.submit(
                    analyze_opportunity,
                    item.symbol,
                    item.asset_type,
                    config,
                    context={**base_context, "runtime_caches": {}},
                    metadata_override=override,
                )
                future_map[future] = item
            for future in as_completed(future_map):
                item = future_map[future]
                try:
                    analysis = future.result()
                except Exception as exc:
                    blind_spots.append(_client_safe_issue(f"{item.symbol} ({item.name}) 扫描失败", exc))
                    continue
                if analysis["excluded"]:
                    continue
                passed += 1
                analyses.append(analysis)
    else:
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
                blind_spots.append(_client_safe_issue(f"{item.symbol} ({item.name}) 扫描失败", exc))
                continue
            if analysis["excluded"]:
                continue
            passed += 1
            analyses.append(analysis)
    analyses.sort(
        key=lambda item: (
            item["rating"]["rank"],
            sum((dimension.get("score") or 0) for dimension in item["dimensions"].values()),
            item["dimensions"]["risk"]["score"] or 0,
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
        "data_coverage": _pick_coverage_state(analyses),
        "coverage_analyses": analyses,
        "top": analyses[:top_n],
        "market_proxy": dict(context.get("global_flow") or {}),
        "proxy_contract": summarize_proxy_contracts_from_analyses(
            analyses,
            market_proxy=context.get("global_flow"),
        ),
        "blind_spots": blind_spots[:8],
        "theme_filter": theme_filter,
        "style_filter": style_filter,
        "manager_filter": manager_filter,
        "preferred_sectors": preferred_sectors,
    }


def compare_opportunities(symbols: Sequence[str], config: Mapping[str, Any]) -> Dict[str, Any]:
    from src.storage.portfolio import build_candidate_set_linkage_summary
    from src.storage.strategy import StrategyRepository

    asset_types = [detect_asset_type_for_compare(symbol, config) for symbol in symbols]
    context = build_market_context(config, relevant_asset_types=list(dict.fromkeys(asset_types + ["cn_etf", "futures"])))
    rows: List[Dict[str, Any]] = []
    try:
        strategy_repository = StrategyRepository()
    except Exception:
        strategy_repository = None
    for symbol, asset_type in zip(symbols, asset_types):
        analysis = analyze_opportunity(symbol, asset_type, config, context=context)
        if strategy_repository is not None:
            try:
                confidence = dict(strategy_repository.summarize_background_confidence(symbol) or {})
            except Exception:
                confidence = {}
            if confidence:
                analysis["strategy_background_confidence"] = confidence
        rows.append(analysis)
    best = max(
        rows,
        key=lambda item: (
            item["rating"]["rank"],
            sum((dimension.get("score") or 0) for dimension in item["dimensions"].values()),
        ),
    )
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "analyses": rows,
        "best_symbol": best["symbol"],
        "compare_linkage_summary": build_candidate_set_linkage_summary(rows),
    }


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
    ("农业", ("农业", "农林牧渔", "种植", "种业", "农药", "化肥", "农化", "饲料", "养殖", "生猪", "钾肥")),
    ("军工", ("军工", "国防", "航空", "航天", "船舶", "兵器")),
    ("金融", ("银行", "保险", "证券", "券商", "多元金融", "非银金融")),
    ("高股息", ("公用事业",)),
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
    ("金融", ("银行", "保险", "证券", "券商")),
]:
    for _kw in _keywords:
        _STOCK_NAME_TO_SECTOR[_kw] = _sector_name

_POWER_EQUIPMENT_STOCK_KEYWORDS = ("电力设备", "电气设备", "储能", "光伏", "风电", "新能源", "逆变器", "电池")
_POWER_EQUIPMENT_STOCK_CHAIN_NODES = ["光伏主链", "储能", "电网设备"]


def _map_industry_to_sector(industry: str, stock_name: str = "") -> tuple[str, List[str]]:
    """Map a stock's industry classification to engine sector + chain_nodes."""
    text_blob = f"{industry} {stock_name}".strip()

    def _financial_chain_nodes(text: str) -> List[str]:
        nodes: List[str] = []
        lowered = str(text or "").lower()
        if any(token in lowered for token in ("证券", "券商", "非银", "broker")):
            nodes.append("券商")
        if "保险" in lowered:
            nodes.append("保险")
        if "银行" in lowered:
            nodes.append("银行")
        return nodes or ["金融"]

    # Priority 1: match via EM industry classification
    for keyword, sector in _INDUSTRY_TO_SECTOR.items():
        if keyword in industry:
            if sector == "金融":
                return sector, _financial_chain_nodes(text_blob)
            if sector == "电网" and any(token in industry for token in _POWER_EQUIPMENT_STOCK_KEYWORDS):
                return "电力设备", list(_POWER_EQUIPMENT_STOCK_CHAIN_NODES)
            _, chain_nodes = _normalize_sector(industry, sector)
            return sector, chain_nodes
    # Priority 2: match via stock name keywords
    for keyword, sector in _STOCK_NAME_TO_SECTOR.items():
        if keyword in stock_name:
            if sector == "金融":
                return sector, _financial_chain_nodes(text_blob)
            if sector == "电网" and any(token in stock_name for token in _POWER_EQUIPMENT_STOCK_KEYWORDS):
                return "电力设备", list(_POWER_EQUIPMENT_STOCK_CHAIN_NODES)
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
    prefer_cached_realtime = bool(dict(config).get("stock_pool_prefer_cached_realtime_runtime", False))

    # --- A-share stocks ---
    if market in {"cn", "all"}:
        try:
            market_collector = ChinaMarketCollector(config)
            realtime = None
            if prefer_cached_realtime:
                realtime = market_collector.get_cached_stock_realtime_snapshot()
            if realtime is None or getattr(realtime, "empty", False):
                realtime = market_collector.get_stock_realtime()
            code_col = "代码" if "代码" in realtime.columns else None
            name_col = "名称" if "名称" in realtime.columns else None
            amount_col = "成交额" if "成交额" in realtime.columns else None
            cap_col = "总市值" if "总市值" in realtime.columns else None
            pe_ttm_col = next((c for c in ("市盈率TTM", "滚动市盈率", "PE滚动", "PE_TTM") if c in realtime.columns), None)
            pe_dynamic_col = next((c for c in ("市盈率(动态)", "动态市盈率") if c in realtime.columns), None)
            pe_raw_col = next((c for c in ("市盈率",) if c in realtime.columns), None)
            pb_col = next((c for c in realtime.columns if "市净率" in c), None)
            industry_col = next((c for c in realtime.columns if c in ("行业", "所属行业")), None)
            bak_strength_col = next((c for c in ("强弱度", "strength") if c in realtime.columns), None)
            bak_activity_col = next((c for c in ("活跃度", "activity") if c in realtime.columns), None)
            bak_attack_col = next((c for c in ("攻击度", "attack") if c in realtime.columns), None)
            bak_swing_col = next((c for c in ("振幅", "swing") if c in realtime.columns), None)
            area_col = next((c for c in ("地域", "area") if c in realtime.columns), None)

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
                frame["_pool_sector"] = frame.apply(
                    lambda row: _map_industry_to_sector(
                        str(row[industry_col]) if industry_col and pd.notna(row.get(industry_col)) else "",
                        str(row[name_col]),
                    )[0],
                    axis=1,
                )
                frame = frame.sort_values(amount_col, ascending=False)
                frame = _sector_round_robin_head(
                    frame,
                    sector_col="_pool_sector",
                    limit=max_candidates,
                )

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
                    if not industry and not bool(dict(config).get("stock_pool_skip_industry_lookup_runtime", False)):
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
                    if bak_strength_col and pd.notna(row.get(bak_strength_col)):
                        meta["bak_strength"] = float(row[bak_strength_col])
                    if bak_activity_col and pd.notna(row.get(bak_activity_col)):
                        meta["bak_activity"] = float(row[bak_activity_col])
                    if bak_attack_col and pd.notna(row.get(bak_attack_col)):
                        meta["bak_attack"] = float(row[bak_attack_col])
                    if bak_swing_col and pd.notna(row.get(bak_swing_col)):
                        meta["bak_swing"] = float(row[bak_swing_col])
                    if area_col and pd.notna(row.get(area_col)):
                        meta["area"] = str(row[area_col])
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
    context: Optional[Mapping[str, Any]] = None,
    *,
    max_candidates: Optional[int] = None,
    attach_signal_confidence: bool = True,
) -> Dict[str, Any]:
    """Scan a stock universe and surface top picks."""
    def _stock_pick_rank_key(analysis: Mapping[str, Any]) -> tuple[float, float, float, float]:
        dimensions = dict(analysis.get("dimensions") or {})
        macro = float(dict(dimensions.get("macro") or {}).get("score") or 0)
        return (
            float(int(dict(analysis.get("rating") or {}).get("rank", 0) or 0)),
            float(sum(float(dict(dimension).get("score") or 0) for dimension in dimensions.values()) + 2 * macro),
            float(dict(dimensions.get("technical") or {}).get("score") or 0),
            float(dict(dimensions.get("fundamental") or {}).get("score") or 0),
        )

    def _qualified_watch_candidate(analysis: Mapping[str, Any]) -> bool:
        dimensions = dict(analysis.get("dimensions") or {})
        technical = float(dict(dimensions.get("technical") or {}).get("score") or 0)
        fundamental = float(dict(dimensions.get("fundamental") or {}).get("score") or 0)
        catalyst = float(dict(dimensions.get("catalyst") or {}).get("score") or 0)
        relative = float(dict(dimensions.get("relative_strength") or {}).get("score") or 0)
        risk = float(dict(dimensions.get("risk") or {}).get("score") or 0)
        support_dims = sum(score >= 60 for score in (technical, fundamental, catalyst, relative, risk))
        positive_dims = sum(score >= 60 for score in (fundamental, catalyst, relative, risk))
        elite_positive = max(fundamental, catalyst, relative, risk) >= 80
        return bool(
            positive_dims >= 2
            or (elite_positive and technical >= 35)
            or (fundamental >= 75 and catalyst >= 30 and technical >= 30)
            or (catalyst >= 60 and relative >= 45 and technical >= 25)
            or (relative >= 70 and technical >= 45)
            or support_dims >= 3
        )

    def _specific_day_theme() -> Dict[str, Any]:
        day_theme = dict(runtime_context.get("day_theme") or {})
        labels = _day_theme_labels(day_theme)
        if not labels:
            return {}
        if not any(_theme_alignment_tokens_for_label(label) for label in labels):
            return {}
        return day_theme

    def _analysis_matches_day_theme(analysis: Mapping[str, Any], day_theme: Mapping[str, Any]) -> bool:
        metadata = dict(analysis.get("metadata") or {})
        metadata.setdefault("name", str(analysis.get("name", "")).strip())
        metadata.setdefault("sector", str(analysis.get("sector", "")).strip())
        return _theme_alignment_level(metadata, dict(day_theme or {})) in {"direct", "indirect"}

    def _meaningful_theme_candidate(analysis: Mapping[str, Any]) -> bool:
        return int(dict(analysis.get("rating") or {}).get("rank", 0) or 0) >= 1 or _qualified_watch_candidate(analysis)

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
        theme_count = 0
        degraded_count = 0
        for item in rows:
            asset_type = str(item.get("asset_type", "")).strip()
            coverage = dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {})
            if coverage.get("structured_event") or coverage.get("forward_event"):
                structured_count += 1
            elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and (
                coverage.get("directional_catalyst_hit")
                or int(coverage.get("theme_news_count") or 0) > 0
                or int(coverage.get("recent_theme_news_pool_count") or 0) > 0
            ):
                structured_count += 1
                theme_count += 1
            if coverage.get("high_confidence_company_news"):
                direct_count += 1
            if coverage.get("degraded") and not (
                asset_type in {"cn_etf", "cn_fund", "cn_index"}
                and (
                    coverage.get("directional_catalyst_hit")
                    or int(coverage.get("theme_news_count") or 0) > 0
                    or str(coverage.get("diagnosis", "")).strip() in {"theme_only_live", "confirmed_live"}
                )
            ):
                degraded_count += 1
        total = len(rows)
        return {
            "news_mode": news_mode,
            "degraded": (news_mode != "live" and theme_count == 0) or degraded_count > 0,
            "structured_rate": structured_count / total if total else 0.0,
            "direct_news_rate": direct_count / total if total else 0.0,
            "summary": f"结构化事件覆盖 {structured_count}/{total}，高置信公司新闻覆盖 {direct_count}/{total}。",
        }

    relevant_by_market = {
        "cn": ["cn_stock", "cn_etf", "futures"],
        "hk": ["hk", "cn_etf", "futures"],
        "us": ["us", "cn_etf", "futures"],
        "all": ["cn_stock", "cn_etf", "hk", "us", "futures"],
    }
    relevant_types = list(dict.fromkeys(relevant_by_market.get(str(market), relevant_by_market["all"])))
    runtime_context = context if context is not None else build_market_context(config, relevant_asset_types=relevant_types)
    opportunity_cfg = dict(dict(config).get("opportunity") or {})
    candidate_limit = int(max_candidates or opportunity_cfg.get("stock_max_scan_candidates", 60) or 60)
    pool, pool_warnings = build_stock_pool(config, market=market, sector_filter=sector_filter, max_candidates=candidate_limit)
    passed = 0
    analyses: List[Dict[str, Any]] = []
    coverage_analyses: List[Dict[str, Any]] = []
    blind_spots: List[str] = list(pool_warnings)
    analysis_workers = max(1, min(int(opportunity_cfg.get("analysis_workers", 4) or 4), len(pool) or 1, 6))
    base_context = dict(runtime_context)

    def _timed_stock_analysis(item: PoolItem, *, runtime_context_override: Mapping[str, Any]) -> Dict[str, Any]:
        override: Dict[str, Any] = {
            "name": item.name,
            "sector": item.sector,
            "chain_nodes": item.chain_nodes,
            "region": item.region,
            "in_watchlist": item.in_watchlist,
        }
        if item.metadata:
            override.update(item.metadata)

        return analyze_opportunity(
            item.symbol,
            item.asset_type,
            config,
            context=runtime_context_override,
            metadata_override=override,
        )

    if analysis_workers > 1 and len(pool) > 1:
        with ThreadPoolExecutor(max_workers=analysis_workers) as executor:
            future_map = {}
            for item in pool:
                future = executor.submit(
                    _timed_stock_analysis,
                    item,
                    runtime_context_override={**base_context, "runtime_caches": {}},
                )
                future_map[future] = item
            for future in as_completed(future_map):
                item = future_map[future]
                try:
                    analysis = future.result()
                except Exception as exc:
                    blind_spots.append(_client_safe_issue(f"{item.symbol} ({item.name}) 扫描失败", exc))
                    continue
                coverage_analyses.append(analysis)
                if analysis["excluded"]:
                    continue
                passed += 1
                analyses.append(analysis)
    else:
        for item in pool:
            try:
                analysis = _timed_stock_analysis(
                    item,
                    runtime_context_override=runtime_context,
                )
            except Exception as exc:
                blind_spots.append(_client_safe_issue(f"{item.symbol} ({item.name}) 扫描失败", exc))
                continue
            coverage_analyses.append(analysis)
            if analysis["excluded"]:
                continue
            passed += 1
            analyses.append(analysis)
    analyses.sort(
        key=_stock_pick_rank_key,
        reverse=True,
    )
    coverage_analyses.sort(key=_stock_pick_rank_key, reverse=True)
    watch_positive = [
        analysis
        for analysis in coverage_analyses
        if int(analysis.get("rating", {}).get("rank", 0) or 0) < 3
        and _qualified_watch_candidate(analysis)
    ]
    top_candidates = analyses[:top_n]
    if not top_candidates:
        top_candidates = watch_positive[:top_n] or coverage_analyses[:top_n]
    theme_gate_applied = False
    theme_gate_reason = ""
    day_theme_for_gate = _specific_day_theme()
    if day_theme_for_gate and not str(sector_filter).strip():
        aligned_passed = [
            analysis
            for analysis in analyses
            if _analysis_matches_day_theme(analysis, day_theme_for_gate) and _meaningful_theme_candidate(analysis)
        ]
        aligned_observe = [
            analysis
            for analysis in coverage_analyses
            if _analysis_matches_day_theme(analysis, day_theme_for_gate) and _meaningful_theme_candidate(analysis)
        ]
        if aligned_passed:
            top_candidates = aligned_passed[:top_n]
            theme_gate_applied = True
            theme_gate_reason = "今日主线有可跟踪候选，首页前排优先保持在主线内。"
        elif aligned_observe:
            top_candidates = aligned_observe[:top_n]
            theme_gate_applied = True
            theme_gate_reason = "今日主线候选触发估值/拥挤等约束，首页改为主线观察稿，不退到非主线寻找形式上的推荐。"
    confidence_targets: List[Dict[str, Any]] = []
    seen_symbols: set[str] = set()
    for bucket in (top_candidates, watch_positive[:6]):
        for analysis in bucket:
            symbol = str(analysis.get("symbol", "")).strip()
            if not symbol or symbol in seen_symbols:
                continue
            confidence_targets.append(analysis)
            seen_symbols.add(symbol)
    if attach_signal_confidence and not bool(dict(config).get("skip_signal_confidence_runtime", False)):
        _attach_signal_confidence(confidence_targets, config, limit=len(confidence_targets))
    market_labels = {"cn": "A 股", "hk": "港股", "us": "美股", "all": "全市场"}
    return {
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "scan_pool": len(pool),
        "passed_pool": passed,
        "market": market,
        "market_label": market_labels.get(market, market),
        "requested_top_n": top_n,
        "regime": runtime_context.get("regime", {}),
        "day_theme": runtime_context.get("day_theme", {}),
        "data_coverage": _coverage_state(coverage_analyses),
        "coverage_analyses": coverage_analyses,
        "top": top_candidates,
        "watch_positive": watch_positive[:6],
        "market_proxy": dict(runtime_context.get("global_flow") or {}),
        "proxy_contract": summarize_proxy_contracts_from_analyses(
            analyses,
            market_proxy=runtime_context.get("global_flow"),
        ),
        "blind_spots": blind_spots[:10],
        "selection_context": {
            "theme_gate_applied": theme_gate_applied,
            "theme_gate_reason": theme_gate_reason,
            "day_theme": dict(day_theme_for_gate or {}),
        },
        "sector_filter": sector_filter,
        "candidate_limit": candidate_limit,
    }
