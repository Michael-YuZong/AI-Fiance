"""Shared fund/ETF taxonomy helpers."""

from __future__ import annotations

import re
from typing import Any, Dict, Mapping, Sequence


SEMICONDUCTOR_KEYWORDS = (
    "半导体",
    "芯片",
    "集成电路",
    "晶圆",
    "晶圆代工",
    "先进封装",
    "封测",
    "存储",
    "hbm",
    "chiplet",
)
SEMICONDUCTOR_CHAIN_NODES = ["半导体", "芯片", "国产替代"]

SEMICONDUCTOR_EQUIPMENT_KEYWORDS = (
    "半导体设备",
    "半导体材料",
    "材料设备",
    "设备材料",
    "光刻机",
    "刻蚀",
    "薄膜沉积",
    "量测",
    "封装设备",
    "晶圆设备",
)
SEMICONDUCTOR_EQUIPMENT_CHAIN_NODES = ["半导体设备", "半导体材料", "国产替代"]

CHIP_KEYWORDS = (
    "科创芯片",
    "芯片",
    "集成电路",
    "先进封装",
    "hbm",
    "chiplet",
    "存储芯片",
)
CHIP_CHAIN_NODES = ["芯片", "半导体", "国产替代"]

SATELLITE_COMMUNICATION_KEYWORDS = (
    "卫星通信",
    "卫星互联网",
    "商用卫星",
    "低轨卫星",
)
SATELLITE_COMMUNICATION_CHAIN_NODES = ["卫星通信", "卫星互联网", "商业航天"]

COMMUNICATION_DEVICE_KEYWORDS = (
    "通信设备",
    "光模块",
    "光通信",
    "cpo",
    "共封装光学",
    "中际旭创",
    "新易盛",
    "天孚通信",
    "源杰科技",
    "太辰光",
    "剑桥科技",
    "光迅科技",
    "博创科技",
    "德科立",
    "联特科技",
)
COMMUNICATION_DEVICE_CHAIN_NODES = ["通信设备", "光模块", "CPO"]

DATA_CENTER_COMMUNICATION_KEYWORDS = (
    "数据中心",
    "idc",
    "交换机",
    "以太网",
)
DATA_CENTER_COMMUNICATION_CHAIN_NODES = ["数据中心", "通信设备", "AI算力"]

CARRIER_COMMUNICATION_KEYWORDS = (
    "运营商",
    "电信",
    "5g",
    "5g-a",
    "6g",
    "云网",
    "万兆光网",
)
CARRIER_COMMUNICATION_CHAIN_NODES = ["运营商", "通信服务", "5G/6G"]

COMMUNICATION_KEYWORDS = (
    *SATELLITE_COMMUNICATION_KEYWORDS,
    *COMMUNICATION_DEVICE_KEYWORDS,
    *DATA_CENTER_COMMUNICATION_KEYWORDS,
    *CARRIER_COMMUNICATION_KEYWORDS,
    "通信",
)
COMMUNICATION_CHAIN_NODES = ["通信设备", "通信服务", "网络基础设施"]

MEDIA_KEYWORDS = (
    "传媒",
    "游戏",
    "动漫",
    "电竞",
    "影视",
    "文娱",
    "广告",
    "出版",
    "aigc",
    "ai应用",
    "元宇宙",
)
MEDIA_CHAIN_NODES = ["游戏", "传媒", "AI应用"]

GAME_MEDIA_KEYWORDS = (
    "游戏",
    "动漫游戏",
    "电竞",
    "手游",
    "网络游戏",
    "版号",
)
GAME_MEDIA_CHAIN_NODES = ["游戏", "传媒", "AI应用"]

BROAD_TECH_KEYWORDS = (
    "科技",
    "ai",
    "人工智能",
    "软件",
    "算力",
    "云计算",
    "互联网",
    "恒生科技",
    "机器人",
    "自动化",
    "智能制造",
    "消费电子",
)
BROAD_TECH_CHAIN_NODES = ["AI算力", "软件服务", "成长股估值修复"]

HK_INNOVATIVE_DRUG_KEYWORDS = ("港股创新药", "香港创新药", "中证香港创新药", "恒生创新药", "港股通创新药")
INNOVATIVE_DRUG_KEYWORDS = ("创新药", "新药", "license-out", "bd授权", "临床", "fda", "asco", "esmo")
INNOVATIVE_DRUG_CHAIN_NODES = ["创新药", "医药研发", "BD授权"]
CXO_KEYWORDS = ("cxo", "cro", "cdmo", "医药外包", "临床外包")
CXO_CHAIN_NODES = ["CXO", "CRO/CDMO", "医药外包"]
MEDICAL_DEVICE_KEYWORDS = ("医疗器械", "医疗设备", "高端医疗", "体外诊断", "ivd", "设备更新")
MEDICAL_DEVICE_CHAIN_NODES = ["医疗器械", "设备更新", "老龄化"]

SMART_GRID_KEYWORDS = ("智能电网", "电网设备", "特高压", "配电网", "虚拟电厂", "输变电", "变压器")
SMART_GRID_CHAIN_NODES = ["特高压", "智能电网", "电网设备"]
GRID_STORAGE_KEYWORDS = ("储能并网", "新型储能", "储能", "逆变器", "电力储能")
GRID_STORAGE_CHAIN_NODES = ["储能并网", "新型储能", "电力设备"]
POWER_EQUIPMENT_KEYWORDS = ("电力设备", "新能源设备", "电气设备", "电力装备")
POWER_EQUIPMENT_CHAIN_NODES = ["电力设备", "智能电网", "储能并网"]


FUND_TAXONOMY_RULES = [
    (HK_INNOVATIVE_DRUG_KEYWORDS, ("医药", ["创新药", "港股医药", "FDA"])),
    (INNOVATIVE_DRUG_KEYWORDS, ("医药", INNOVATIVE_DRUG_CHAIN_NODES)),
    (CXO_KEYWORDS, ("医药", CXO_CHAIN_NODES)),
    (MEDICAL_DEVICE_KEYWORDS, ("医药", MEDICAL_DEVICE_CHAIN_NODES)),
    (SEMICONDUCTOR_EQUIPMENT_KEYWORDS, ("半导体", SEMICONDUCTOR_EQUIPMENT_CHAIN_NODES)),
    (CHIP_KEYWORDS, ("半导体", CHIP_CHAIN_NODES)),
    (SEMICONDUCTOR_KEYWORDS, ("半导体", SEMICONDUCTOR_CHAIN_NODES)),
    (SATELLITE_COMMUNICATION_KEYWORDS, ("通信", SATELLITE_COMMUNICATION_CHAIN_NODES)),
    (COMMUNICATION_DEVICE_KEYWORDS, ("通信", COMMUNICATION_DEVICE_CHAIN_NODES)),
    (DATA_CENTER_COMMUNICATION_KEYWORDS, ("通信", DATA_CENTER_COMMUNICATION_CHAIN_NODES)),
    (CARRIER_COMMUNICATION_KEYWORDS, ("通信", CARRIER_COMMUNICATION_CHAIN_NODES)),
    (COMMUNICATION_KEYWORDS, ("通信", COMMUNICATION_CHAIN_NODES)),
    (GAME_MEDIA_KEYWORDS, ("传媒", GAME_MEDIA_CHAIN_NODES)),
    (MEDIA_KEYWORDS, ("传媒", MEDIA_CHAIN_NODES)),
    (BROAD_TECH_KEYWORDS, ("科技", BROAD_TECH_CHAIN_NODES)),
    (("军工", "国防", "航天", "卫星", "商业航天"), ("军工", ["军工", "地缘风险", "商业航天"])),
    (("黄金", "贵金属"), ("黄金", ["黄金", "通胀预期"])),
    (SMART_GRID_KEYWORDS, ("电网", SMART_GRID_CHAIN_NODES)),
    (GRID_STORAGE_KEYWORDS, ("电网", GRID_STORAGE_CHAIN_NODES)),
    (POWER_EQUIPMENT_KEYWORDS, ("电网", POWER_EQUIPMENT_CHAIN_NODES)),
    (("电网", "电力"), ("电网", ["电力需求", "智能电网", "电网设备"])),
    (("有色", "铜", "铝", "黄金股"), ("有色", ["铜铝", "顺周期"])),
    (("医药", "医疗", "生物医药", "制药"), ("医药", ["医药", "老龄化"])),
    (("农业", "农牧", "农林", "粮食", "粮油", "种业", "种植", "农化", "化肥", "农资", "粮食安全", "乡村振兴"), ("农业", ["粮食安全", "种业", "农化"])),
    (("消费", "食品", "饮料", "家电", "零售", "消费龙头"), ("消费", ["内需", "消费修复"])),
    (("红利", "高股息", "股息", "公用事业"), ("高股息", ["高股息", "防守"])),
    (("金融", "银行", "保险", "证券", "券商", "非银", "broker", "brokerage"), ("金融", ["券商", "银行", "资本市场"])),
    (("能源", "原油", "煤炭", "油气", "能化", "化工"), ("能源", ["原油", "能源安全", "通胀预期"])),
    (
        (
            "沪深300",
            "中证a500",
            "a500",
            "中证500",
            "上证50",
            "上证综指",
            "上证综合",
            "上证指数",
            "恒生指数",
            "恒生中国企业指数",
            "国企指数",
            "恒指",
            "hang seng index",
            "宽基",
        ),
        ("宽基", ["宽基", "大盘蓝筹", "内需"]),
    ),
]

OVERSEAS_KEYWORDS = ("nasdaq", "纳斯达克", "标普", "sp500", "s&p", "港股", "美股", "hong kong", "qdii", "海外")

THEME_PROFILE_RULES: list[tuple[tuple[str, ...], Dict[str, Any]]] = [
    (
        COMMUNICATION_DEVICE_KEYWORDS,
        {
            "theme_family": "硬科技",
            "primary_chain": "CPO/光模块",
            "theme_role": "AI硬件主链",
            "theme_directness": "direct",
            "evidence_keywords": ["CPO", "光模块", "光通信", "共封装光学", "800G", "1.6T", "AI算力", "数据中心"],
            "preferred_sector_aliases": ["科技", "硬科技", "AI硬件", "AI算力", "通信", "通信设备", "CPO", "光模块"],
            "mainline_tags": ["AI硬件链", "通信设备", "算力基础设施"],
        },
    ),
    (
        DATA_CENTER_COMMUNICATION_KEYWORDS,
        {
            "theme_family": "硬科技",
            "primary_chain": "数据中心网络",
            "theme_role": "AI硬件主链",
            "theme_directness": "direct",
            "evidence_keywords": ["数据中心", "IDC", "交换机", "以太网", "AI算力", "液冷", "服务器"],
            "preferred_sector_aliases": ["科技", "硬科技", "AI硬件", "AI算力", "通信", "数据中心"],
            "mainline_tags": ["AI硬件链", "数据中心", "网络基础设施"],
        },
    ),
    (
        SEMICONDUCTOR_EQUIPMENT_KEYWORDS,
        {
            "theme_family": "硬科技",
            "primary_chain": "半导体设备",
            "theme_role": "AI硬件主链",
            "theme_directness": "direct",
            "evidence_keywords": ["半导体设备", "光刻机", "刻蚀", "薄膜沉积", "量测", "国产替代", "晶圆厂扩产"],
            "preferred_sector_aliases": ["科技", "硬科技", "AI硬件", "半导体", "芯片", "半导体设备"],
            "mainline_tags": ["AI硬件链", "国产替代", "设备材料"],
        },
    ),
    (
        CHIP_KEYWORDS + SEMICONDUCTOR_KEYWORDS,
        {
            "theme_family": "硬科技",
            "primary_chain": "芯片/半导体",
            "theme_role": "AI硬件主链",
            "theme_directness": "direct",
            "evidence_keywords": ["芯片", "半导体", "先进封装", "HBM", "Chiplet", "存储", "集成电路", "国产替代"],
            "preferred_sector_aliases": ["科技", "硬科技", "AI硬件", "半导体", "芯片", "AI算力"],
            "mainline_tags": ["AI硬件链", "国产替代", "算力基础设施"],
        },
    ),
    (
        SATELLITE_COMMUNICATION_KEYWORDS,
        {
            "theme_family": "商业航天",
            "primary_chain": "卫星互联网",
            "theme_role": "通信侧翼",
            "theme_directness": "adjacent",
            "evidence_keywords": ["卫星通信", "卫星互联网", "低轨卫星", "商业航天", "火箭", "星座"],
            "preferred_sector_aliases": ["通信", "卫星互联网", "商业航天", "军工", "科技"],
            "mainline_tags": ["通信基础设施", "商业航天"],
        },
    ),
    (
        CARRIER_COMMUNICATION_KEYWORDS,
        {
            "theme_family": "数字基建",
            "primary_chain": "运营商/5G-6G",
            "theme_role": "通信基础设施",
            "theme_directness": "adjacent",
            "evidence_keywords": ["运营商", "5G", "5G-A", "6G", "云网", "万兆光网", "资本开支"],
            "preferred_sector_aliases": ["通信", "运营商", "数字基建", "科技", "AI算力"],
            "mainline_tags": ["通信基础设施", "数字基建"],
        },
    ),
    (
        SMART_GRID_KEYWORDS,
        {
            "theme_family": "新型电力系统",
            "primary_chain": "智能电网/特高压",
            "theme_role": "AI电力侧链",
            "theme_directness": "sidechain",
            "evidence_keywords": ["智能电网", "特高压", "配电网", "虚拟电厂", "输变电", "电网投资", "AI用电"],
            "preferred_sector_aliases": ["电网", "电力设备", "新型电力系统", "智能电网", "特高压", "AI算力"],
            "mainline_tags": ["AI电力侧链", "电网投资"],
        },
    ),
    (
        GRID_STORAGE_KEYWORDS,
        {
            "theme_family": "新型电力系统",
            "primary_chain": "储能并网",
            "theme_role": "AI电力侧链",
            "theme_directness": "sidechain",
            "evidence_keywords": ["储能", "储能并网", "新型储能", "逆变器", "虚拟电厂", "电力储能", "AI用电"],
            "preferred_sector_aliases": ["电网", "电力设备", "储能", "新型电力系统", "AI算力"],
            "mainline_tags": ["AI电力侧链", "储能并网"],
        },
    ),
    (
        POWER_EQUIPMENT_KEYWORDS,
        {
            "theme_family": "新型电力系统",
            "primary_chain": "电力设备",
            "theme_role": "AI电力侧链",
            "theme_directness": "sidechain",
            "evidence_keywords": ["电力设备", "电力装备", "新能源设备", "智能电网", "储能并网", "AI用电"],
            "preferred_sector_aliases": ["电网", "电力设备", "新型电力系统", "储能", "AI算力"],
            "mainline_tags": ["AI电力侧链", "电力设备"],
        },
    ),
    (
        HK_INNOVATIVE_DRUG_KEYWORDS + INNOVATIVE_DRUG_KEYWORDS,
        {
            "theme_family": "医药成长",
            "primary_chain": "创新药",
            "theme_role": "创新药主线",
            "theme_directness": "non_ai",
            "evidence_keywords": ["创新药", "新药", "license-out", "BD授权", "临床", "FDA", "ASCO", "ESMO", "医保谈判"],
            "preferred_sector_aliases": ["医药", "创新药", "医药成长", "港股医药", "生物医药"],
            "mainline_tags": ["创新药", "医药研发", "BD出海"],
        },
    ),
    (
        CXO_KEYWORDS,
        {
            "theme_family": "医药成长",
            "primary_chain": "CXO",
            "theme_role": "创新药服务链",
            "theme_directness": "non_ai",
            "evidence_keywords": ["CXO", "CRO", "CDMO", "医药外包", "临床外包", "订单", "产能利用率"],
            "preferred_sector_aliases": ["医药", "CXO", "CRO", "CDMO", "医药成长"],
            "mainline_tags": ["创新药服务链", "医药外包"],
        },
    ),
    (
        MEDICAL_DEVICE_KEYWORDS,
        {
            "theme_family": "医药成长",
            "primary_chain": "医疗器械",
            "theme_role": "设备更新/医疗消费",
            "theme_directness": "non_ai",
            "evidence_keywords": ["医疗器械", "医疗设备", "设备更新", "IVD", "高端医疗", "招标采购"],
            "preferred_sector_aliases": ["医药", "医疗器械", "医疗设备", "设备更新"],
            "mainline_tags": ["医疗器械", "设备更新"],
        },
    ),
    (
        GAME_MEDIA_KEYWORDS + MEDIA_KEYWORDS,
        {
            "theme_family": "传媒应用",
            "primary_chain": "游戏/AI应用",
            "theme_role": "AI应用修复",
            "theme_directness": "application",
            "evidence_keywords": ["游戏", "版号", "AIGC", "AI应用", "动漫", "影视", "电竞", "内容出海"],
            "preferred_sector_aliases": ["传媒", "游戏", "AI应用", "科技", "文娱"],
            "mainline_tags": ["AI应用", "游戏传媒"],
        },
    ),
    (
        BROAD_TECH_KEYWORDS,
        {
            "theme_family": "泛科技",
            "primary_chain": "AI/成长科技",
            "theme_role": "科技成长主线",
            "theme_directness": "broad",
            "evidence_keywords": ["AI", "人工智能", "算力", "软件", "云计算", "机器人", "智能制造"],
            "preferred_sector_aliases": ["科技", "硬科技", "AI", "人工智能", "AI算力", "成长"],
            "mainline_tags": ["科技成长", "AI主题"],
        },
    ),
]

SECTOR_THEME_DEFAULTS: Dict[str, Dict[str, Any]] = {
    "宽基": {
        "theme_family": "宽基市场",
        "primary_chain": "宽基指数",
        "theme_role": "市场风险偏好",
        "theme_directness": "broad_market",
        "evidence_keywords": ["宽基", "沪深300", "中证A500", "上证50", "指数"],
        "preferred_sector_aliases": ["宽基", "大盘", "A股", "市场"],
        "mainline_tags": ["市场贝塔"],
    },
    "通信": {
        "theme_family": "硬科技",
        "primary_chain": "通信设备",
        "theme_role": "AI硬件主链",
        "theme_directness": "direct",
        "evidence_keywords": ["通信", "通信设备", "光模块", "CPO", "AI算力", "5G", "6G"],
        "preferred_sector_aliases": ["科技", "硬科技", "AI硬件", "通信", "AI算力"],
        "mainline_tags": ["AI硬件链", "通信基础设施"],
    },
    "半导体": {
        "theme_family": "硬科技",
        "primary_chain": "半导体",
        "theme_role": "AI硬件主链",
        "theme_directness": "direct",
        "evidence_keywords": ["半导体", "芯片", "国产替代", "先进封装", "AI算力"],
        "preferred_sector_aliases": ["科技", "硬科技", "AI硬件", "半导体", "芯片"],
        "mainline_tags": ["AI硬件链", "国产替代"],
    },
    "科技": {
        "theme_family": "泛科技",
        "primary_chain": "AI/成长科技",
        "theme_role": "科技成长主线",
        "theme_directness": "broad",
        "evidence_keywords": ["科技", "AI", "人工智能", "算力", "软件", "云计算"],
        "preferred_sector_aliases": ["科技", "硬科技", "AI", "AI算力", "成长"],
        "mainline_tags": ["科技成长"],
    },
    "电网": {
        "theme_family": "新型电力系统",
        "primary_chain": "电网设备",
        "theme_role": "AI电力侧链",
        "theme_directness": "sidechain",
        "evidence_keywords": ["电网", "电网设备", "特高压", "智能电网", "储能", "AI用电"],
        "preferred_sector_aliases": ["电网", "电力设备", "新型电力系统", "智能电网", "AI算力"],
        "mainline_tags": ["AI电力侧链", "电网投资"],
    },
    "电力设备": {
        "theme_family": "新型电力系统",
        "primary_chain": "光伏/储能/电力设备",
        "theme_role": "AI电力侧链",
        "theme_directness": "sidechain",
        "evidence_keywords": ["电力设备", "光伏", "储能", "逆变器", "新能源设备", "电网设备", "AI用电"],
        "preferred_sector_aliases": ["电力设备", "电网", "储能", "光伏", "新型电力系统", "AI算力"],
        "mainline_tags": ["AI电力侧链", "电力设备"],
    },
    "医药": {
        "theme_family": "医药成长",
        "primary_chain": "医药",
        "theme_role": "医药修复",
        "theme_directness": "non_ai",
        "evidence_keywords": ["医药", "医疗", "创新药", "医保", "临床"],
        "preferred_sector_aliases": ["医药", "医疗", "创新药", "医药成长"],
        "mainline_tags": ["医药成长"],
    },
    "传媒": {
        "theme_family": "传媒应用",
        "primary_chain": "游戏/传媒",
        "theme_role": "AI应用修复",
        "theme_directness": "application",
        "evidence_keywords": ["传媒", "游戏", "版号", "AIGC", "AI应用"],
        "preferred_sector_aliases": ["传媒", "游戏", "AI应用", "科技"],
        "mainline_tags": ["AI应用", "游戏传媒"],
    },
    "黄金": {
        "theme_family": "避险资产",
        "primary_chain": "黄金",
        "theme_role": "避险/利率交易",
        "theme_directness": "macro",
        "evidence_keywords": ["黄金", "贵金属", "金价", "美元", "实际利率", "地缘"],
        "preferred_sector_aliases": ["黄金", "贵金属", "避险"],
        "mainline_tags": ["避险资产"],
    },
    "有色": {
        "theme_family": "资源周期",
        "primary_chain": "有色金属",
        "theme_role": "顺周期/资源品",
        "theme_directness": "cyclical",
        "evidence_keywords": ["有色", "铜", "铝", "工业金属", "库存", "价格"],
        "preferred_sector_aliases": ["有色", "工业金属", "资源周期"],
        "mainline_tags": ["资源周期"],
    },
    "能源": {
        "theme_family": "资源周期",
        "primary_chain": "能源",
        "theme_role": "能源价格交易",
        "theme_directness": "cyclical",
        "evidence_keywords": ["能源", "原油", "油气", "煤炭", "能化", "地缘"],
        "preferred_sector_aliases": ["能源", "油气", "煤炭", "能化", "资源周期"],
        "mainline_tags": ["资源周期"],
    },
    "高股息": {
        "theme_family": "防守红利",
        "primary_chain": "高股息",
        "theme_role": "防守配置",
        "theme_directness": "defensive",
        "evidence_keywords": ["高股息", "红利", "分红", "股息率", "防守"],
        "preferred_sector_aliases": ["高股息", "红利", "防守"],
        "mainline_tags": ["防守配置"],
    },
    "消费": {
        "theme_family": "消费内需",
        "primary_chain": "消费",
        "theme_role": "内需修复",
        "theme_directness": "domestic_demand",
        "evidence_keywords": ["消费", "内需", "食品饮料", "白酒", "家电", "零售"],
        "preferred_sector_aliases": ["消费", "内需", "食品饮料"],
        "mainline_tags": ["内需修复"],
    },
    "农业": {
        "theme_family": "农业安全",
        "primary_chain": "农业",
        "theme_role": "粮食安全/周期防守",
        "theme_directness": "defensive",
        "evidence_keywords": ["农业", "种业", "粮食安全", "农化", "化肥"],
        "preferred_sector_aliases": ["农业", "种业", "粮食安全"],
        "mainline_tags": ["粮食安全"],
    },
    "军工": {
        "theme_family": "军工安全",
        "primary_chain": "军工",
        "theme_role": "地缘/装备周期",
        "theme_directness": "geopolitical",
        "evidence_keywords": ["军工", "国防", "装备", "航天", "地缘"],
        "preferred_sector_aliases": ["军工", "国防", "商业航天"],
        "mainline_tags": ["军工安全"],
    },
    "金融": {
        "theme_family": "金融地产链",
        "primary_chain": "金融",
        "theme_role": "政策/资本市场交易",
        "theme_directness": "policy_beta",
        "evidence_keywords": ["金融", "银行", "券商", "证券", "保险", "资本市场"],
        "preferred_sector_aliases": ["金融", "银行", "券商", "证券"],
        "mainline_tags": ["资本市场"],
    },
}


def _unique_strings(values: Sequence[Any]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text or text in seen:
            continue
        seen.add(text)
        result.append(text)
    return result


def _theme_detection_text(text: str) -> str:
    cleaned = str(text or "")
    for noise in (
        "中国人民银行人民币活期存款利率",
        "银行活期存款利率",
        "人民币活期存款利率",
        "活期存款税后利率",
        "活期存款利率",
        "税后",
    ):
        cleaned = cleaned.replace(noise, " ")
    return cleaned


def _match_taxonomy_rule(text: str) -> tuple[str, list[str]]:
    lowered = _theme_detection_text(text).lower()
    for keywords, payload in FUND_TAXONOMY_RULES:
        if any(keyword.lower() in lowered for keyword in keywords):
            return payload[0], list(payload[1])
    return "", []


def _theme_profile_blob(*values: Any) -> str:
    parts: list[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            parts.append(_theme_profile_blob(*value.values()))
        elif isinstance(value, (list, tuple, set)):
            parts.append(_theme_profile_blob(*value))
        else:
            text = str(value or "").strip()
            if text:
                parts.append(text)
    return _theme_detection_text(" ".join(parts))


def _complete_theme_profile(
    payload: Mapping[str, Any],
    *,
    sector: str,
    chain_nodes: Sequence[str],
    confidence: str,
) -> Dict[str, Any]:
    cleaned_sector = str(sector or "").strip() or "综合"
    cleaned_nodes = _unique_strings(chain_nodes)
    primary_chain = str(payload.get("primary_chain", "")).strip()
    if not primary_chain:
        primary_chain = next((node for node in cleaned_nodes if node != cleaned_sector), cleaned_sector)
    theme_family = str(payload.get("theme_family", "")).strip() or cleaned_sector
    theme_role = str(payload.get("theme_role", "")).strip() or theme_family
    evidence_keywords = _unique_strings(
        [
            primary_chain,
            *cleaned_nodes,
            *list(payload.get("evidence_keywords") or []),
        ]
    )
    preferred_aliases = _unique_strings(
        [
            cleaned_sector,
            theme_family,
            theme_role,
            primary_chain,
            *cleaned_nodes,
            *list(payload.get("preferred_sector_aliases") or []),
        ]
    )
    mainline_tags = _unique_strings(list(payload.get("mainline_tags") or []))
    return {
        "theme_family": theme_family,
        "primary_chain": primary_chain,
        "theme_role": theme_role,
        "theme_directness": str(payload.get("theme_directness", "")).strip() or "unknown",
        "evidence_keywords": evidence_keywords[:24],
        "preferred_sector_aliases": preferred_aliases[:24],
        "mainline_tags": mainline_tags[:12],
        "taxonomy_profile_confidence": confidence,
    }


def build_theme_taxonomy_profile(
    *,
    sector: str,
    chain_nodes: Sequence[str] = (),
    name: str = "",
    benchmark: str = "",
    tracking_target: str = "",
    labels: Sequence[str] = (),
) -> Dict[str, Any]:
    """Return upper-layer theme semantics for downstream ranking and evidence matching.

    ``sector`` and ``chain_nodes`` are the lower-level classification. This profile
    adds the market-language layer that report selection needs: aliases, evidence
    terms, and whether the theme is core AI hardware, sidechain, application, or
    unrelated to AI.
    """
    cleaned_sector = str(sector or "").strip() or "综合"
    cleaned_nodes = _unique_strings(chain_nodes)
    blob = _theme_profile_blob(name, benchmark, tracking_target, labels, cleaned_sector, cleaned_nodes).lower()
    for keywords, payload in THEME_PROFILE_RULES:
        if any(str(keyword or "").lower() in blob for keyword in keywords):
            return _complete_theme_profile(
                payload,
                sector=cleaned_sector,
                chain_nodes=cleaned_nodes,
                confidence="rule",
            )
    default_payload = SECTOR_THEME_DEFAULTS.get(cleaned_sector)
    if default_payload:
        return _complete_theme_profile(
            default_payload,
            sector=cleaned_sector,
            chain_nodes=cleaned_nodes,
            confidence="sector",
        )
    return _complete_theme_profile(
        {
            "theme_family": cleaned_sector if cleaned_sector != "综合" else "综合配置",
            "primary_chain": next((node for node in cleaned_nodes if node != cleaned_sector), cleaned_sector),
            "theme_role": "观察配置",
            "theme_directness": "unknown",
            "evidence_keywords": cleaned_nodes,
            "preferred_sector_aliases": [cleaned_sector, *cleaned_nodes],
            "mainline_tags": [],
        },
        sector=cleaned_sector,
        chain_nodes=cleaned_nodes,
        confidence="fallback",
    )


def infer_fund_sector(text: str, sector_hint: str = "") -> tuple[str, list[str]]:
    sector, chain_nodes = _match_taxonomy_rule(str(text or ""))
    if sector:
        return sector, chain_nodes

    sector, chain_nodes = _match_taxonomy_rule(str(sector_hint or ""))
    if sector:
        return sector, chain_nodes

    return "综合", ["主动管理", "组合配置"]


def infer_share_class(name: str) -> str:
    normalized = str(name or "").strip().upper()
    if not normalized:
        return "未识别"
    if "联接" in normalized:
        for marker, label in (("C", "C类"), ("A", "A类"), ("E", "E类"), ("I", "I类"), ("Y", "Y类"), ("F", "F类"), ("B", "B类")):
            if normalized.endswith(marker):
                return f"ETF联接{label}"
        return "ETF联接"
    if "ETF" in normalized:
        return "未分级"
    for marker, label in (("C", "C类"), ("A", "A类"), ("E", "E类"), ("I", "I类"), ("Y", "Y类"), ("F", "F类"), ("B", "B类")):
        if normalized.endswith(marker):
            return label
    return "未分级"


def uses_index_mainline(payload: Mapping[str, Any]) -> bool:
    asset_type = str(payload.get("asset_type", "")).strip()
    if asset_type in {"cn_etf", "cn_index"}:
        return True
    if asset_type != "cn_fund":
        return False

    if bool(payload.get("is_passive_fund")):
        return True

    metadata = dict(payload.get("metadata") or {})
    if bool(metadata.get("is_passive_fund")):
        return True

    management_style = str(payload.get("fund_management_style", "") or metadata.get("fund_management_style", "")).strip()
    if management_style in {"被动跟踪", "指数增强"}:
        return True
    if management_style == "主动管理":
        return False

    fund_profile = dict(payload.get("fund_profile") or {})
    style = dict(fund_profile.get("style") or {})
    tags = [str(item).strip() for item in list(style.get("tags") or []) if str(item).strip()]
    if "被动跟踪" in tags:
        return True
    if "主动管理" in tags:
        return False

    taxonomy = dict(style.get("taxonomy") or {})
    taxonomy_management = str(taxonomy.get("management_style", "")).strip()
    if taxonomy_management in {"被动跟踪", "指数增强"}:
        return True
    if taxonomy_management == "主动管理":
        return False

    overview = dict(fund_profile.get("overview") or {})
    combined = " ".join(
        [
            str(payload.get("name", "")).strip(),
            str(overview.get("基金简称", "")).strip(),
            str(overview.get("基金类型", "")).strip(),
            management_style,
            taxonomy_management,
        ]
    ).lower()
    return any(token in combined for token in ("被动", "指数型", "etf", "联接", "增强指数"))


def _contains_overseas_token(text: str) -> bool:
    lowered = str(text or "").strip().lower()
    if not lowered:
        return False
    if any(token in lowered for token in OVERSEAS_KEYWORDS):
        return True
    if "恒生" in lowered and "a股" not in lowered:
        return True
    return False


def _benchmark_overseas_weight(benchmark: str) -> float:
    total = 0.0
    for segment in re.split(r"[+＋]", str(benchmark or "")):
        if not _contains_overseas_token(segment):
            continue
        match = re.search(r"[*x×X]\s*(\d+(?:\.\d+)?)\s*%", segment)
        if match:
            total += float(match.group(1))
        else:
            total += 100.0
    return total


def build_standard_fund_taxonomy(
    *,
    name: str,
    fund_type: str = "",
    invest_type: str = "",
    benchmark: str = "",
    tracking_target: str = "",
    asset_type: str = "cn_fund",
    sector_hint: str = "",
    is_passive: bool | None = None,
    commodity_like: bool | None = None,
) -> Dict[str, Any]:
    name_text = str(name or "").strip()
    benchmark_text = str(benchmark or "").strip()
    invest_text = str(invest_type or "").strip()
    fund_type_text = str(fund_type or "").strip()
    tracking_text = str(tracking_target or "").strip()
    combined = " ".join([name_text, fund_type_text, invest_text, benchmark_text, tracking_text])
    lowered = _theme_detection_text(combined).lower()

    passive = bool(is_passive) if is_passive is not None else any(token in lowered for token in ("指数", "etf", "联接", "被动"))
    commodity = bool(commodity_like) if commodity_like is not None else any(
        token in lowered for token in ("商品", "期货", "原油", "黄金", "贵金属", "能源化工", "现货", "合约")
    )
    explicit_overseas = any(
        _contains_overseas_token(text)
        for text in (name_text, fund_type_text, invest_text, tracking_text)
    )
    benchmark_overseas_weight = _benchmark_overseas_weight(benchmark_text)
    overseas = explicit_overseas or benchmark_overseas_weight >= 30.0
    sector, chain_nodes = infer_fund_sector(combined, sector_hint=sector_hint)
    hint_sector, hint_chain_nodes = _match_taxonomy_rule(str(sector_hint or ""))
    if sector in {"综合", "科技"} and hint_sector not in {"", "综合", "科技"}:
        sector, chain_nodes = hint_sector, hint_chain_nodes

    if "创新药" in lowered and sector == "医药":
        if overseas:
            chain_nodes = ["创新药", "港股医药", "FDA"]
        else:
            chain_nodes = ["创新药", "医药研发", "BD授权"]

    if commodity:
        exposure_scope = "商品"
    elif overseas:
        exposure_scope = "跨境"
    elif sector == "宽基":
        exposure_scope = "宽基"
    elif sector != "综合":
        exposure_scope = "行业主题"
    else:
        exposure_scope = "综合"

    if asset_type == "cn_fund":
        product_form = "场外基金"
    elif asset_type == "cn_etf":
        product_form = "ETF"
    elif asset_type in {"hk_index", "cn_index"}:
        product_form = "指数代理"
    elif asset_type == "futures":
        product_form = "期货代理"
    elif asset_type in {"us", "hk"}:
        product_form = "海外ETF" if any(token in lowered for token in ("etf", "trust", "shares", "fund")) else "海外代理"
    else:
        product_form = "基金/代理"

    if asset_type == "futures":
        vehicle_role = "商品期货代理"
    elif asset_type in {"hk_index", "cn_index"}:
        vehicle_role = "指数代理"
    elif "联接" in name_text:
        vehicle_role = "ETF联接"
    elif asset_type == "cn_etf":
        vehicle_role = "场内ETF"
    elif asset_type == "cn_fund":
        vehicle_role = "开放式基金"
    elif asset_type in {"us", "hk"}:
        vehicle_role = "海外ETF代理"
    else:
        vehicle_role = "代理标的"

    if asset_type in {"futures", "hk_index", "cn_index"}:
        management_style = "代理跟踪"
    elif "增强" in combined:
        management_style = "指数增强"
    elif passive:
        management_style = "被动跟踪"
    else:
        management_style = "主动管理"

    if commodity:
        benchmark_kind = "现货/期货"
    elif "指数" in combined:
        benchmark_kind = "指数"
    elif asset_type in {"futures", "hk_index", "cn_index"}:
        benchmark_kind = "代理基准"
    else:
        benchmark_kind = "业绩比较基准"

    share_class = infer_share_class(name_text)
    labels = [
        product_form,
        vehicle_role,
        management_style,
        exposure_scope if exposure_scope != "综合" else sector,
    ]
    if sector != "综合":
        labels.append(f"{sector}方向")
    if share_class not in {"未识别", "未分级"}:
        labels.append(share_class)
    labels = [str(item).strip() for item in labels if str(item).strip()]
    theme_profile = build_theme_taxonomy_profile(
        sector=sector,
        chain_nodes=chain_nodes,
        name=name_text,
        benchmark=benchmark_text,
        tracking_target=tracking_text,
        labels=labels,
    )

    summary = (
        f"这只标的按统一分类更接近 `{product_form} / {vehicle_role} / {management_style}`，"
        f"主暴露属于 `{exposure_scope if exposure_scope != '综合' else sector}`。"
    )

    return {
        "product_form": product_form,
        "vehicle_role": vehicle_role,
        "management_style": management_style,
        "exposure_scope": exposure_scope,
        "sector": sector,
        "chain_nodes": chain_nodes,
        "benchmark_kind": benchmark_kind,
        "share_class": share_class,
        "labels": labels[:6],
        "summary": summary,
        "theme_profile": theme_profile,
        "theme_family": theme_profile.get("theme_family", ""),
        "primary_chain": theme_profile.get("primary_chain", ""),
        "theme_role": theme_profile.get("theme_role", ""),
        "theme_directness": theme_profile.get("theme_directness", ""),
        "evidence_keywords": list(theme_profile.get("evidence_keywords") or []),
        "preferred_sector_aliases": list(theme_profile.get("preferred_sector_aliases") or []),
        "mainline_tags": list(theme_profile.get("mainline_tags") or []),
        "taxonomy_profile_confidence": theme_profile.get("taxonomy_profile_confidence", ""),
    }


def taxonomy_rows(taxonomy: Mapping[str, Any]) -> list[list[str]]:
    payload = dict(taxonomy or {})
    rows = [
        ["产品形态", str(payload.get("product_form", "—"))],
        ["载体角色", str(payload.get("vehicle_role", "—"))],
        ["管理方式", str(payload.get("management_style", "—"))],
        ["暴露类型", str(payload.get("exposure_scope", "—"))],
        ["主方向", str(payload.get("sector", "—"))],
        ["主线族群", str(payload.get("theme_family", "—"))],
        ["主链条", str(payload.get("primary_chain", "—"))],
        ["链路角色", str(payload.get("theme_role", "—"))],
        ["份额类别", str(payload.get("share_class", "—"))],
    ]
    return rows


def taxonomy_from_analysis(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    fund_profile = dict(analysis.get("fund_profile") or {})
    style = dict(fund_profile.get("style") or {})
    taxonomy = dict(style.get("taxonomy") or {})
    if taxonomy:
        if not taxonomy.get("theme_profile"):
            profile = build_theme_taxonomy_profile(
                sector=str(taxonomy.get("sector", "")),
                chain_nodes=list(taxonomy.get("chain_nodes") or []),
                name=str(analysis.get("name", "")),
                benchmark=str(dict(analysis.get("metadata") or {}).get("benchmark", "")),
                labels=list(taxonomy.get("labels") or []),
            )
            taxonomy["theme_profile"] = profile
            for key in (
                "theme_family",
                "primary_chain",
                "theme_role",
                "theme_directness",
                "evidence_keywords",
                "preferred_sector_aliases",
                "mainline_tags",
                "taxonomy_profile_confidence",
            ):
                taxonomy.setdefault(key, profile.get(key))
        return taxonomy
    overview = dict(fund_profile.get("overview") or {})
    metadata = dict(analysis.get("metadata") or {})
    return build_standard_fund_taxonomy(
        name=str(analysis.get("name", metadata.get("name", ""))),
        fund_type=str(overview.get("基金类型", metadata.get("fund_type", ""))),
        invest_type=str(metadata.get("invest_type", "")),
        benchmark=str(overview.get("业绩比较基准", metadata.get("benchmark", ""))),
        tracking_target=str(overview.get("跟踪标的", "")),
        asset_type=str(analysis.get("asset_type", metadata.get("asset_type", "cn_fund"))),
        sector_hint=str(metadata.get("sector", style.get("sector", ""))),
        is_passive=bool(metadata.get("is_passive_fund")) if metadata.get("is_passive_fund") is not None else None,
    )
