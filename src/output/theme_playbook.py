"""Theme playbook loading and matching helpers for thesis-first homepage writing."""

from __future__ import annotations

from pathlib import Path
import re
from typing import Any, Dict, List, Mapping

from src.utils.config import resolve_project_path


HARD_SECTOR_REGISTRY: Dict[str, Dict[str, Any]] = {
    "information_technology": {
        "label": "信息技术",
        "path": "docs/theme_playbooks/sector_information_technology.md",
        "aliases": (
            "信息技术",
            "科技",
            "电子",
            "计算机",
            "软件",
            "半导体",
            "通信设备",
            "互联网",
        ),
    },
    "health_care": {
        "label": "医疗保健",
        "path": "docs/theme_playbooks/sector_health_care.md",
        "aliases": (
            "医疗保健",
            "医药",
            "医疗",
            "创新药",
            "cxo",
            "cro",
            "器械",
        ),
    },
    "financials": {
        "label": "金融",
        "path": "docs/theme_playbooks/sector_financials.md",
        "aliases": (
            "金融",
            "银行",
            "证券",
            "券商",
            "保险",
            "多元金融",
        ),
    },
    "consumer_discretionary": {
        "label": "可选消费",
        "path": "docs/theme_playbooks/sector_consumer_discretionary.md",
        "aliases": (
            "可选消费",
            "消费",
            "白酒",
            "家电",
            "汽车",
            "免税",
            "餐饮",
            "文旅",
        ),
    },
    "consumer_staples": {
        "label": "必选消费",
        "path": "docs/theme_playbooks/sector_consumer_staples.md",
        "aliases": (
            "必选消费",
            "食品",
            "乳制品",
            "调味品",
        ),
    },
    "agriculture": {
        "label": "农业 / 种植链",
        "path": "docs/theme_playbooks/sector_agriculture.md",
        "aliases": (
            "农业",
            "农林牧渔",
            "种植业",
            "种业",
            "农业种植",
            "农产品",
            "养殖",
            "饲料",
            "生猪",
            "玉米",
            "大豆",
        ),
    },
    "industrials": {
        "label": "工业",
        "path": "docs/theme_playbooks/sector_industrials.md",
        "aliases": (
            "工业",
            "军工",
            "机械",
            "机器人",
            "自动驾驶",
            "低空经济",
            "卫星互联网",
            "一带一路",
        ),
    },
    "materials": {
        "label": "材料",
        "path": "docs/theme_playbooks/sector_materials.md",
        "aliases": (
            "材料",
            "有色",
            "黄金",
            "铜",
            "铝",
            "稀土",
            "化工",
            "建材",
            "固态电池",
            "钙钛矿",
        ),
    },
    "energy": {
        "label": "能源",
        "path": "docs/theme_playbooks/sector_energy.md",
        "aliases": (
            "能源",
            "原油",
            "油气",
            "煤炭",
            "电力",
            "绿电",
            "风电",
            "光伏",
        ),
    },
    "utilities": {
        "label": "公用事业",
        "path": "docs/theme_playbooks/sector_utilities.md",
        "aliases": (
            "公用事业",
            "电网",
            "水务",
            "燃气",
            "运营商",
            "红利低波",
        ),
    },
    "real_estate": {
        "label": "房地产",
        "path": "docs/theme_playbooks/sector_real_estate.md",
        "aliases": (
            "房地产",
            "地产",
            "地产链",
            "物业",
            "房屋租赁",
            "城中村改造",
        ),
    },
    "communication_services": {
        "label": "通信服务",
        "path": "docs/theme_playbooks/sector_communication_services.md",
        "aliases": (
            "通信服务",
            "传媒",
            "通信",
            "运营商",
            "广告营销",
            "短视频",
            "游戏",
        ),
    },
    "market_beta": {
        "label": "宽基 / 市场Beta",
        "aliases": (
            "宽基",
            "大盘",
            "市场beta",
            "市场 beta",
            "沪深300",
            "中证500",
            "中证1000",
            "上证50",
            "a50",
            "a500",
            "中证a500",
            "创业板",
            "创业板指",
            "科创50",
        ),
    },
}


PLAYBOOK_REGISTRY: Dict[str, Dict[str, Any]] = {
    "semiconductor": {
        "label": "半导体",
        "path": "docs/theme_playbooks/semiconductor.md",
        "keywords": (
            "半导体",
            "芯片",
            "晶圆",
            "存储",
            "封装",
        ),
    },
    "advanced_packaging": {
        "label": "先进封装 / HBM / Chiplet",
        "path": "docs/theme_playbooks/advanced_packaging.md",
        "keywords": (
            "先进封装",
            "hbm",
            "chiplet",
            "cowos",
            "封装测试",
            "载板",
            "先进封装设备",
        ),
    },
    "ai_computing": {
        "label": "AI算力",
        "path": "docs/theme_playbooks/ai_computing.md",
        "keywords": (
            "ai算力",
            "算力",
            "光模块",
            "服务器",
            "交换机",
            "液冷",
            "cpo",
            "推理",
            "大模型",
        ),
    },
    "solid_state_battery": {
        "label": "固态电池",
        "path": "docs/theme_playbooks/solid_state_battery.md",
        "keywords": (
            "固态电池",
            "全固态",
            "半固态",
            "硫化物",
            "氧化物电解质",
            "锂金属",
        ),
    },
    "perovskite": {
        "label": "钙钛矿",
        "path": "docs/theme_playbooks/perovskite.md",
        "keywords": (
            "钙钛矿",
            "叠层电池",
            "蒸镀",
            "光伏新技术",
            "钙钛矿电池",
        ),
    },
    "quantum_computing": {
        "label": "量子计算",
        "path": "docs/theme_playbooks/quantum_computing.md",
        "keywords": (
            "量子计算",
            "量子科技",
            "量子通信",
            "量子芯片",
            "量子测量",
        ),
    },
    "solar_mainchain": {
        "label": "光伏主链",
        "path": "docs/theme_playbooks/solar_mainchain.md",
        "keywords": (
            "光伏",
            "组件",
            "逆变器",
            "硅料",
            "硅片",
            "电池片",
            "装机",
        ),
    },
    "energy_storage": {
        "label": "储能",
        "path": "docs/theme_playbooks/energy_storage.md",
        "keywords": (
            "储能",
            "大储",
            "工商业储能",
            "pcs",
            "储能系统",
            "温控",
            "储能电芯",
        ),
    },
    "lithium_battery": {
        "label": "锂电",
        "path": "docs/theme_playbooks/lithium_battery.md",
        "keywords": (
            "锂电",
            "锂电池",
            "正极",
            "负极",
            "电解液",
            "隔膜",
            "六氟磷酸锂",
            "锂矿",
        ),
    },
    "power_grid": {
        "label": "电网设备",
        "path": "docs/theme_playbooks/power_grid.md",
        "keywords": (
            "电网",
            "特高压",
            "配网",
            "变压器",
            "电表",
            "柔直",
            "电力设备",
        ),
    },
    "robotics_mobility": {
        "label": "机器人与智能驾驶",
        "path": "docs/theme_playbooks/robotics_mobility.md",
        "keywords": (
            "机器人",
            "人形机器人",
            "减速器",
            "丝杠",
        ),
    },
    "autonomous_driving": {
        "label": "自动驾驶",
        "path": "docs/theme_playbooks/autonomous_driving.md",
        "keywords": (
            "自动驾驶",
            "智驾",
            "车路云",
            "激光雷达",
            "线控底盘",
            "noa",
            "城市领航",
        ),
    },
    "low_altitude_satellite": {
        "label": "低空经济 / 卫星互联网",
        "path": "docs/theme_playbooks/low_altitude_satellite.md",
        "keywords": (
            "低空经济",
            "evtol",
            "卫星互联网",
            "卫星通信",
            "商业航天",
            "通航",
            "火箭",
            "卫星",
            "空域",
        ),
    },
    "energy_resources": {
        "label": "能源 / 资源",
        "path": "docs/theme_playbooks/energy_resources.md",
        "keywords": (
            "原油",
            "油气",
            "资源",
            "能源",
            "opec",
            "中东",
            "库存周期",
            "供给扰动",
        ),
    },
    "coal_energy_security": {
        "label": "煤炭 / 能源安全",
        "path": "docs/theme_playbooks/coal_energy_security.md",
        "keywords": (
            "煤炭",
            "动力煤",
            "焦煤",
            "煤价",
            "火电",
            "能源安全",
            "煤电",
        ),
    },
    "gold_nonferrous": {
        "label": "黄金 / 有色资源",
        "path": "docs/theme_playbooks/gold_nonferrous.md",
        "keywords": (
            "黄金",
            "金价",
            "有色",
            "铜",
            "铝",
            "小金属",
            "稀土",
            "锑",
        ),
    },
    "policy_substitution": {
        "label": "政策驱动 / 国产替代",
        "path": "docs/theme_playbooks/policy_substitution.md",
        "keywords": (
            "国产替代",
            "信创",
            "新质生产力",
            "一带一路",
            "专精特新",
            "碳中和",
            "绿电",
            "自主可控",
            "卡脖子",
        ),
    },
    "data_elements": {
        "label": "数据要素",
        "path": "docs/theme_playbooks/data_elements.md",
        "keywords": (
            "数据要素",
            "数据资产",
            "公共数据",
            "语料",
            "国资云",
            "数据流通",
        ),
    },
    "central_soe_revaluation": {
        "label": "央企市值管理 / 中特估",
        "path": "docs/theme_playbooks/central_soe_revaluation.md",
        "keywords": (
            "央企市值管理",
            "中特估",
            "国企改革",
            "央企",
            "市值管理",
            "估值重塑",
            "国企",
        ),
    },
    "hstech": {
        "label": "恒生科技",
        "path": "docs/theme_playbooks/hstech.md",
        "keywords": (
            "恒生科技",
            "港股科技",
            "平台经济",
            "互联网",
            "腾讯",
            "阿里",
            "美团",
            "快手",
            "字节",
            "南下",
            "港股互联网",
        ),
    },
    "innovative_drug": {
        "label": "创新药",
        "path": "docs/theme_playbooks/innovative_drug.md",
        "keywords": (
            "创新药",
            "biotech",
            "adc",
            "双抗",
            "创新药主题",
        ),
    },
    "bd_out_licensing": {
        "label": "BD出海 / 授权交易",
        "path": "docs/theme_playbooks/bd_out_licensing.md",
        "keywords": (
            "bd",
            "授权",
            "license-out",
            "出海",
            "海外合作",
            "首付款",
            "里程碑",
            "royalty",
            "国际化",
        ),
    },
    "clinical_readout": {
        "label": "临床催化 / 管线读数",
        "path": "docs/theme_playbooks/clinical_readout.md",
        "keywords": (
            "临床",
            "readout",
            "iii期",
            "ii期",
            "asco",
            "aacr",
            "esmo",
            "数据读出",
            "入组",
            "终点",
            "管线",
        ),
    },
    "cxo_service": {
        "label": "CRO / CXO",
        "path": "docs/theme_playbooks/cxo_service.md",
        "keywords": (
            "cxo",
            "cro",
            "cdmo",
            "实验室服务",
            "订单",
            "产能利用率",
            "海外客户",
            "生物安全法",
            "地缘扰动",
        ),
    },
    "consumer_recovery": {
        "label": "消费复苏",
        "path": "docs/theme_playbooks/consumer_recovery.md",
        "keywords": (
            "消费复苏",
            "可选消费",
            "大众消费",
            "内需修复",
            "消费修复",
        ),
    },
    "baijiu": {
        "label": "白酒 / 高端消费",
        "path": "docs/theme_playbooks/baijiu.md",
        "keywords": (
            "白酒",
            "高端白酒",
            "次高端",
            "茅台",
            "五粮液",
            "泸州老窖",
            "山西汾酒",
            "渠道库存",
            "动销",
            "提价",
        ),
    },
    "home_appliance": {
        "label": "家电 / 出海制造消费",
        "path": "docs/theme_playbooks/home_appliance.md",
        "keywords": (
            "家电",
            "白电",
            "黑电",
            "空调",
            "冰箱",
            "洗衣机",
            "小家电",
            "出海",
            "外销",
            "海外渠道",
            "以旧换新",
        ),
    },
    "travel_retail": {
        "label": "免税 / 文旅 / 出行链",
        "path": "docs/theme_playbooks/travel_retail.md",
        "keywords": (
            "免税",
            "文旅",
            "旅游",
            "酒店",
            "景区",
            "航空",
            "机场",
            "出行",
            "客流",
            "票价",
            "暑期",
            "节假日",
        ),
    },
    "mass_consumption": {
        "label": "餐饮供应链 / 大众消费",
        "path": "docs/theme_playbooks/consumer_recovery.md",
        "keywords": (
            "餐饮",
            "预制菜",
            "啤酒",
            "乳制品",
            "调味品",
            "商超",
            "社零",
            "大众消费",
            "刚需消费",
            "性价比",
        ),
    },
    "real_estate_recovery": {
        "label": "地产链复苏",
        "path": "docs/theme_playbooks/real_estate_recovery.md",
        "keywords": (
            "地产链复苏",
            "房地产",
            "地产链",
            "建材",
            "家居",
            "物业",
            "竣工",
            "保交楼",
        ),
    },
    "dividend_value": {
        "label": "高股息 / 红利",
        "path": "docs/theme_playbooks/dividend_value.md",
        "keywords": (
            "高股息",
            "红利",
            "红利低波",
            "股息率",
            "分红",
            "防守",
            "低波",
        ),
    },
    "market_beta": {
        "label": "宽基 / 市场Beta",
        "path": "docs/theme_playbooks/market_beta.md",
        "keywords": (
            "宽基",
            "大盘",
            "市场beta",
            "市场 beta",
            "a500",
            "中证a500",
            "沪深300",
            "中证500",
            "中证1000",
            "上证50",
            "a50",
            "创业板",
            "创业板指",
            "科创50",
        ),
    },
    "huawei_chain": {
        "label": "华为链",
        "path": "docs/theme_playbooks/huawei_chain.md",
        "keywords": (
            "华为链",
            "鸿蒙",
            "mate",
            "麒麟",
            "pura",
            "huawei chain",
        ),
    },
    "apple_chain": {
        "label": "苹果链",
        "path": "docs/theme_playbooks/apple_chain.md",
        "keywords": (
            "苹果链",
            "iphone",
            "vision pro",
            "ipad",
            "mac",
            "apple chain",
        ),
    },
    "nvidia_chain": {
        "label": "英伟达链",
        "path": "docs/theme_playbooks/nvidia_chain.md",
        "keywords": (
            "英伟达链",
            "英伟达",
            "nvidia",
            "gb200",
            "blackwell",
            "nvlink",
            "nvidia chain",
        ),
    },
    "tesla_chain": {
        "label": "特斯拉链",
        "path": "docs/theme_playbooks/tesla_chain.md",
        "keywords": (
            "特斯拉链",
            "特斯拉",
            "tesla",
            "fsd",
            "dojo",
            "model y",
            "tesla chain",
        ),
    },
    "event_supply_chain": {
        "label": "事件产业链",
        "path": "docs/theme_playbooks/event_supply_chain.md",
        "keywords": (
            "事件产业链",
            "供应链脉冲",
            "产业链映射",
            "发布会链",
            "事件驱动链",
        ),
    },
}

THEME_FAMILY_MAP: Dict[str, str] = {
    "semiconductor": "技术路线",
    "advanced_packaging": "技术路线",
    "ai_computing": "技术路线",
    "solid_state_battery": "技术路线",
    "perovskite": "技术路线",
    "quantum_computing": "技术路线",
    "solar_mainchain": "周期 / 宏观",
    "energy_storage": "技术路线",
    "lithium_battery": "技术路线",
    "power_grid": "周期 / 宏观",
    "robotics_mobility": "技术路线",
    "autonomous_driving": "技术路线",
    "low_altitude_satellite": "技术路线",
    "energy_resources": "周期 / 宏观",
    "coal_energy_security": "周期 / 宏观",
    "gold_nonferrous": "周期 / 宏观",
    "policy_substitution": "政策驱动",
    "data_elements": "政策驱动",
    "central_soe_revaluation": "政策驱动",
    "market_beta": "周期 / 宏观",
    "hstech": "技术路线",
    "innovative_drug": "技术路线",
    "bd_out_licensing": "技术路线",
    "clinical_readout": "技术路线",
    "cxo_service": "技术路线",
    "consumer_recovery": "周期 / 宏观",
    "baijiu": "周期 / 宏观",
    "home_appliance": "周期 / 宏观",
    "travel_retail": "周期 / 宏观",
    "mass_consumption": "周期 / 宏观",
    "real_estate_recovery": "周期 / 宏观",
    "dividend_value": "周期 / 宏观",
    "huawei_chain": "事件产业链",
    "apple_chain": "事件产业链",
    "nvidia_chain": "事件产业链",
    "tesla_chain": "事件产业链",
    "event_supply_chain": "事件产业链",
}

SECTOR_SUBTHEME_BRIDGE: Dict[str, tuple[str, ...]] = {
    "information_technology": (
        "semiconductor",
        "advanced_packaging",
        "ai_computing",
        "quantum_computing",
        "data_elements",
        "hstech",
    ),
    "health_care": (
        "innovative_drug",
        "bd_out_licensing",
        "clinical_readout",
        "cxo_service",
    ),
    "financials": (
        "central_soe_revaluation",
        "dividend_value",
    ),
    "consumer_discretionary": (
        "consumer_recovery",
        "baijiu",
        "home_appliance",
        "travel_retail",
    ),
    "consumer_staples": (
        "mass_consumption",
    ),
    "agriculture": (),
    "industrials": (
        "robotics_mobility",
        "autonomous_driving",
        "low_altitude_satellite",
        "policy_substitution",
        "event_supply_chain",
    ),
    "materials": (
        "gold_nonferrous",
        "solid_state_battery",
        "perovskite",
        "lithium_battery",
    ),
    "energy": (
        "energy_resources",
        "coal_energy_security",
        "solar_mainchain",
        "energy_storage",
        "power_grid",
    ),
    "utilities": (
        "power_grid",
        "dividend_value",
    ),
    "real_estate": (
        "real_estate_recovery",
    ),
    "communication_services": (
        "hstech",
        "data_elements",
        "event_supply_chain",
    ),
}

THEME_CONFLICT_GROUPS: Dict[str, tuple[str, ...]] = {
    "tech_stack": (
        "semiconductor",
        "advanced_packaging",
        "ai_computing",
    ),
    "biotech_subthemes": (
        "innovative_drug",
        "bd_out_licensing",
        "clinical_readout",
        "cxo_service",
    ),
    "event_chain_subthemes": (
        "huawei_chain",
        "apple_chain",
        "nvidia_chain",
        "event_supply_chain",
    ),
    "consumer_subthemes": (
        "consumer_recovery",
        "baijiu",
        "home_appliance",
        "travel_retail",
        "mass_consumption",
    ),
    "policy_subthemes": (
        "policy_substitution",
        "data_elements",
        "central_soe_revaluation",
    ),
}

THEME_CONFLICT_INDEX: Dict[str, str] = {
    key: group
    for group, keys in THEME_CONFLICT_GROUPS.items()
    for key in keys
}

SECTION_HEADING_MAP = {
    "市场通常在交易什么": "market_logic",
    "常见正向驱动": "bullish_drivers",
    "常见反向风险": "risks",
    "首页应优先联想到的宏观 / 产业变量": "variables",
    "典型传导链": "transmission_path",
    "常见所处阶段": "stage_pattern",
    "轮动与拥挤度": "rotation_and_crowding",
    "证伪信号": "falsifiers",
    "不能误写成直接催化的东西": "guardrails",
    "写首页时应避免的模板化表达": "style_notes",
    # Allow machine-friendly headings in playbook markdown so the parser
    # doesn't silently drop sections when a card is written in key form.
    "market_logic": "market_logic",
    "bullish_drivers": "bullish_drivers",
    "risks": "risks",
    "variables": "variables",
    "transmission_path": "transmission_path",
    "stage_pattern": "stage_pattern",
    "rotation_and_crowding": "rotation_and_crowding",
    "falsifiers": "falsifiers",
    "guardrails": "guardrails",
    "style_notes": "style_notes",
}


def _collect_text(*values: Any) -> str:
    parts: List[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            parts.extend(_collect_text(*value.values()).split("\n"))
            continue
        if isinstance(value, (list, tuple, set)):
            parts.extend(_collect_text(*list(value)).split("\n"))
            continue
        text = str(value).strip()
        if text:
            parts.append(text)
    return "\n".join(part for part in parts if part)


def _leading_text(*values: Any) -> str:
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            text = _leading_text(*value.values())
            if text:
                return text
            continue
        if isinstance(value, (list, tuple, set)):
            text = _leading_text(*list(value))
            if text:
                return text
            continue
        text = str(value).strip()
        if text:
            return text
    return ""


def _score_registry(
    corpus: str,
    registry: Mapping[str, Mapping[str, Any]],
    *,
    field: str,
) -> tuple[str, int]:
    best_key = ""
    best_score = 0
    for key, meta in registry.items():
        score, _ = _match_tokens(corpus, meta.get(field, ()))
        if score > best_score:
            best_key = key
            best_score = score
    return best_key, best_score


def _match_tokens(corpus: str, tokens: Any) -> tuple[int, List[str]]:
    score = 0
    matched: List[str] = []
    counted: set[str] = set()
    for token in tokens or ():
        token_raw = str(token).strip()
        token_text = token_raw.lower()
        if not token_text or token_text not in corpus:
            continue
        if token_text in counted:
            continue
        score += len(token_text)
        counted.add(token_text)
        skip_token = False
        for index, existing in enumerate(list(matched)):
            existing_text = existing.lower()
            if token_text == existing_text or token_text in existing_text:
                skip_token = True
                break
            if existing_text in token_text:
                matched[index] = token_raw
                skip_token = True
                break
        if not skip_token:
            matched.append(token_raw)
    return score, matched


def _parse_sections(markdown_text: str) -> Dict[str, List[str]]:
    sections: Dict[str, List[str]] = {}
    current_key = ""
    current_lines: List[str] = []
    for raw_line in markdown_text.splitlines():
        line = raw_line.rstrip()
        if line.startswith("## "):
            if current_key:
                sections[current_key] = [item for item in current_lines if item]
            heading = line[3:].strip()
            current_key = SECTION_HEADING_MAP.get(heading, "")
            current_lines = []
            continue
        if not current_key:
            continue
        stripped = line.strip()
        if not stripped:
            continue
        if stripped.startswith("- "):
            current_lines.append(stripped[2:].strip())
        else:
            current_lines.append(stripped)
    if current_key:
        sections[current_key] = [item for item in current_lines if item]
    return sections


def load_theme_playbook(key: str) -> Dict[str, Any]:
    meta = dict(PLAYBOOK_REGISTRY.get(key) or {})
    if not meta:
        return {}
    path = resolve_project_path(str(meta.get("path", "")))
    if not Path(path).exists():
        return {}
    text = Path(path).read_text(encoding="utf-8")
    return {
        "key": key,
        "label": meta.get("label", key),
        "path": str(path),
        "theme_family": THEME_FAMILY_MAP.get(key, ""),
        "sections": _parse_sections(text),
    }


def load_sector_playbook(key: str) -> Dict[str, Any]:
    meta = dict(HARD_SECTOR_REGISTRY.get(key) or {})
    path_ref = str(meta.get("path", "")).strip()
    if not meta or not path_ref:
        return {}
    path = resolve_project_path(path_ref)
    if not Path(path).exists():
        return {}
    text = Path(path).read_text(encoding="utf-8")
    return {
        "key": f"sector::{key}",
        "label": meta.get("label", key),
        "path": str(path),
        "theme_family": "行业层",
        "sections": _parse_sections(text),
    }


def rank_theme_playbook_candidates(*values: Any) -> List[Dict[str, Any]]:
    corpus = _collect_text(*values).lower()
    lead_corpus = _leading_text(*values).lower()
    if not corpus:
        return []
    ranked: List[tuple[int, int, Dict[str, Any]]] = []
    for index, (key, meta) in enumerate(PLAYBOOK_REGISTRY.items()):
        score, matched_tokens = _match_tokens(
            corpus,
            [
                meta.get("label", ""),
                *(meta.get("keywords") or ()),
            ],
        )
        lead_score, lead_tokens = _match_tokens(
            lead_corpus,
            [
                meta.get("label", ""),
                *(meta.get("keywords") or ()),
            ],
        )
        if score <= 0:
            continue
        total_score = score + lead_score * 2
        ranked.append(
            (
                -total_score,
                index,
                {
                    "key": key,
                    "label": str(meta.get("label") or key),
                    "score": total_score,
                    "base_score": score,
                    "lead_score": lead_score,
                    "matched_tokens": (lead_tokens + matched_tokens)[:3],
                    "conflict_group": THEME_CONFLICT_INDEX.get(key, ""),
                },
            )
        )
    ranked.sort(key=lambda item: (item[0], item[1]))
    return [payload for _, _, payload in ranked]


def sector_subtheme_bridge_items(hard_sector_key: str, *values: Any, limit: int = 3) -> List[Dict[str, Any]]:
    corpus = _collect_text(*values).lower()
    ranked: List[tuple[int, int, Dict[str, Any]]] = []
    for index, theme_key in enumerate(SECTOR_SUBTHEME_BRIDGE.get(hard_sector_key, ())):
        meta = dict(PLAYBOOK_REGISTRY.get(theme_key) or {})
        if not meta:
            continue
        score, matched_tokens = _match_tokens(
            corpus,
            [
                meta.get("label", ""),
                *(meta.get("keywords") or ()),
            ],
        )
        ranked.append(
            (
                -score,
                index,
                {
                    "key": theme_key,
                    "label": str(meta.get("label") or theme_key),
                    "theme_family": THEME_FAMILY_MAP.get(theme_key, ""),
                    "score": score,
                    "matched_tokens": matched_tokens[:3],
                },
            )
        )
    ranked.sort(key=lambda item: (item[0], item[1]))
    items: List[Dict[str, Any]] = []
    for _, _, payload in ranked[:limit]:
        items.append(
            {
                "key": str(payload.get("key") or ""),
                "label": str(payload.get("label") or ""),
                "theme_family": str(payload.get("theme_family") or ""),
                "score": int(payload.get("score") or 0),
                "matched_tokens": list(payload.get("matched_tokens") or []),
            }
        )
    return items


def summarize_theme_conflict(candidates: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    ranked = [dict(item) for item in candidates if dict(item)]
    if not ranked:
        return {
            "status": "none",
            "reason": "",
            "group": "",
            "labels": [],
        }
    top = dict(ranked[0])
    second = dict(ranked[1]) if len(ranked) > 1 else {}
    top_group = str(top.get("conflict_group") or "")
    second_group = str(second.get("conflict_group") or "")
    top_score = int(top.get("score") or 0)
    second_score = int(second.get("score") or 0)
    top_lead_score = int(top.get("lead_score") or 0)
    top_matches = [str(token).strip() for token in list(top.get("matched_tokens") or []) if str(token).strip()]
    longest_match = max((len(token) for token in top_matches), default=0)
    if not top_group or top_group != second_group or second_score <= 0:
        return {
            "status": "clear",
            "reason": "",
            "group": top_group,
            "labels": [str(top.get("label") or "")],
        }
    score_gap = top_score - second_score
    if top_group == "policy_subthemes":
        if second_score > 0 and top_lead_score <= 0:
            labels = [str(item.get("label") or "") for item in ranked if str(item.get("conflict_group") or "") == top_group][:3]
            labels = [label for label in labels if label]
            return {
                "status": "ambiguous_conflict",
                "reason": f"`{' / '.join(labels)}` 这几条政策子主题当前同时给分，泛政策表达先不要硬落单一细主题。",
                "group": top_group,
                "labels": labels,
            }
    if top_group in {"event_chain_subthemes", "consumer_subthemes"}:
        if second_score > 0 and not (top_score >= 9 and score_gap >= 5 and (top_lead_score > 0 or len(top_matches) >= 2)):
            labels = [str(item.get("label") or "") for item in ranked if str(item.get("conflict_group") or "") == top_group][:3]
            labels = [label for label in labels if label]
            return {
                "status": "ambiguous_conflict",
                "reason": f"`{' / '.join(labels)}` 这几条易混主题当前都在给信号，先不要硬落单一细主题。",
                "group": top_group,
                "labels": labels,
            }
    if top_score < 4 or score_gap <= 1 or (score_gap <= 2 and second_score >= 6 and longest_match < 6) or (
        len(top_matches) <= 1 and score_gap <= 4 and longest_match < 6
    ):
        labels = [str(item.get("label") or "") for item in ranked if str(item.get("conflict_group") or "") == top_group][:3]
        labels = [label for label in labels if label]
        return {
            "status": "ambiguous_conflict",
            "reason": f"`{' / '.join(labels)}` 这几条易混主题当前还没完全拉开，先不要硬落单一细主题。",
            "group": top_group,
            "labels": labels,
        }
    return {
        "status": "clear",
        "reason": "",
        "group": top_group,
        "labels": [str(top.get("label") or "")],
    }


def summarize_sector_subtheme_bridge(items: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    bridge_items = [dict(item) for item in items if dict(item)]
    if not bridge_items:
        return {
            "confidence": "none",
            "reason": "",
            "top_key": "",
            "top_label": "",
            "top_score": 0,
            "gap_to_second": 0,
        }
    top_item = dict(bridge_items[0])
    second_item = dict(bridge_items[1]) if len(bridge_items) > 1 else {}
    top_score = int(top_item.get("score") or 0)
    second_score = int(second_item.get("score") or 0)
    matched_tokens = [str(token).strip() for token in list(top_item.get("matched_tokens") or []) if str(token).strip()]
    gap_to_second = max(top_score - second_score, 0)
    if top_score <= 0 or not matched_tokens:
        confidence = "none"
        reason = "当前还没有足够强的上下文命中，只能把细分方向当成观察清单。"
    elif top_score >= 10 and len(matched_tokens) >= 2 and gap_to_second >= 3:
        confidence = "high"
        reason = f"命中 `{matched_tokens[0]}` / `{matched_tokens[1]}`，且相对次优候选领先 {gap_to_second} 分。"
    elif top_score >= 6:
        confidence = "medium"
        reason = f"已命中 `{matched_tokens[0]}` 等线索，但和其他细分方向还没有完全拉开。"
    else:
        confidence = "low"
        reason = f"只命中了 `{matched_tokens[0]}` 这类单点线索，仍不足以把行业层直接写成细主题。"
    return {
        "confidence": confidence,
        "reason": reason,
        "top_key": str(top_item.get("key") or ""),
        "top_label": str(top_item.get("label") or ""),
        "top_score": top_score,
        "gap_to_second": gap_to_second,
    }


def classify_hard_sector(*values: Any, explicit_key: str = "") -> Dict[str, Any]:
    if explicit_key:
        meta = dict(HARD_SECTOR_REGISTRY.get(explicit_key) or {})
        if meta:
            return {
                "key": explicit_key,
                "label": meta.get("label", explicit_key),
                "aliases": list(meta.get("aliases") or []),
            }
    corpus = _collect_text(*values).lower()
    if not corpus:
        return {}
    best_key, best_score = _score_registry(corpus, HARD_SECTOR_REGISTRY, field="aliases")
    if not best_key or best_score <= 0:
        return {}
    meta = dict(HARD_SECTOR_REGISTRY.get(best_key) or {})
    return {
        "key": best_key,
        "label": meta.get("label", best_key),
        "aliases": list(meta.get("aliases") or []),
    }


def resolve_theme_playbook(*values: Any, explicit_key: str = "") -> Dict[str, Any]:
    if explicit_key:
        loaded = load_theme_playbook(explicit_key)
        if loaded:
            return loaded
    candidates = rank_theme_playbook_candidates(*values)
    if not candidates:
        return {}
    conflict = summarize_theme_conflict(candidates)
    if conflict.get("status") == "ambiguous_conflict":
        return {}
    return load_theme_playbook(str(candidates[0].get("key") or ""))


def build_theme_playbook_context(*values: Any, explicit_key: str = "") -> Dict[str, Any]:
    theme_candidates = rank_theme_playbook_candidates(*values)
    theme_conflict = summarize_theme_conflict(theme_candidates)
    playbook = resolve_theme_playbook(*values, explicit_key=explicit_key)
    hard_sector = classify_hard_sector(*values)
    subtheme_bridge = sector_subtheme_bridge_items(str(hard_sector.get("key", "")), *values)
    bridge_summary = summarize_sector_subtheme_bridge(subtheme_bridge)
    if not playbook:
        if not hard_sector:
            return {}
        sector_playbook = load_sector_playbook(str(hard_sector.get("key", "")))
        if not sector_playbook:
            return {
                "hard_sector_key": hard_sector.get("key", ""),
                "hard_sector_label": hard_sector.get("label", ""),
                "subtheme_bridge": subtheme_bridge,
                "subtheme_bridge_confidence": bridge_summary.get("confidence", "none"),
                "subtheme_bridge_reason": bridge_summary.get("reason", ""),
                "subtheme_bridge_top_key": bridge_summary.get("top_key", ""),
                "subtheme_bridge_top_label": bridge_summary.get("top_label", ""),
                "theme_match_status": theme_conflict.get("status", "sector_only"),
                "theme_match_reason": theme_conflict.get("reason", ""),
                "theme_match_candidates": list(theme_conflict.get("labels") or []),
            }
        playbook = sector_playbook
    sections = dict(playbook.get("sections") or {})
    return {
        "key": playbook.get("key"),
        "label": playbook.get("label"),
        "path": playbook.get("path"),
        "theme_family": playbook.get("theme_family", ""),
        "playbook_level": "sector" if str(playbook.get("key", "")).startswith("sector::") else "theme",
        "hard_sector_key": hard_sector.get("key", ""),
        "hard_sector_label": hard_sector.get("label", ""),
        "subtheme_bridge": subtheme_bridge if str(playbook.get("key", "")).startswith("sector::") else [],
        "subtheme_bridge_confidence": bridge_summary.get("confidence", "none")
        if str(playbook.get("key", "")).startswith("sector::")
        else "",
        "subtheme_bridge_reason": bridge_summary.get("reason", "")
        if str(playbook.get("key", "")).startswith("sector::")
        else "",
        "subtheme_bridge_top_key": bridge_summary.get("top_key", "")
        if str(playbook.get("key", "")).startswith("sector::")
        else "",
        "subtheme_bridge_top_label": bridge_summary.get("top_label", "")
        if str(playbook.get("key", "")).startswith("sector::")
        else "",
        "theme_match_status": (
            "resolved"
            if str(playbook.get("key", "")).strip()
            and not str(playbook.get("key", "")).startswith("sector::")
            else theme_conflict.get("status", "sector_only")
        ),
        "theme_match_reason": (
            ""
            if str(playbook.get("key", "")).strip()
            and not str(playbook.get("key", "")).startswith("sector::")
            else theme_conflict.get("reason", "")
        ),
        "theme_match_candidates": (
            []
            if str(playbook.get("key", "")).strip()
            and not str(playbook.get("key", "")).startswith("sector::")
            else list(theme_conflict.get("labels") or [])
        ),
        "market_logic": list(sections.get("market_logic") or []),
        "bullish_drivers": list(sections.get("bullish_drivers") or []),
        "risks": list(sections.get("risks") or []),
        "variables": list(sections.get("variables") or []),
        "transmission_path": list(sections.get("transmission_path") or []),
        "stage_pattern": list(sections.get("stage_pattern") or []),
        "rotation_and_crowding": list(sections.get("rotation_and_crowding") or []),
        "falsifiers": list(sections.get("falsifiers") or []),
        "guardrails": list(sections.get("guardrails") or []),
        "style_notes": list(sections.get("style_notes") or []),
    }


def playbook_hint_line(playbook: Mapping[str, Any]) -> str:
    label = str(playbook.get("label", "")).strip()
    logic = list(playbook.get("market_logic") or [])
    stage = list(playbook.get("stage_pattern") or [])
    prefix = "从行业层看，" if str(playbook.get("playbook_level", "")).strip() == "sector" else ""

    def _trim_tail(text: str) -> str:
        return str(text or "").strip().rstrip("。；;，, ")

    def _normalize_stage(text: str) -> str:
        line = _trim_tail(text)
        line = re.sub(r"^(更常见的是|常见的是|通常是|往往是|常见于)", "", line).strip()
        return line.replace("“", "").replace("”", "").strip()

    if not label and not logic:
        return ""
    if logic and stage:
        return f"{prefix}{label}当前更像在交易：{_trim_tail(logic[0])}。常见阶段往往是 `{_normalize_stage(stage[0])}`。"
    if logic:
        return f"{prefix}{label}当前更像在交易：{_trim_tail(logic[0])}。"
    if str(playbook.get("playbook_level", "")).strip() == "sector":
        return f"从行业层看，{label}当前需要结合盈利周期、政策和风格一起理解。"
    return f"{label}主题当前需要结合产业链和风格去理解。"
