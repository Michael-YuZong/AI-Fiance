"""Shared fund/ETF taxonomy helpers."""

from __future__ import annotations

import re
from typing import Any, Dict, Mapping, Sequence


FUND_TAXONOMY_RULES = [
    (("科技", "半导体", "芯片", "ai", "人工智能", "软件", "算力", "恒生科技"), ("科技", ["AI算力", "半导体", "成长股估值修复"])),
    (("军工", "国防", "航天", "卫星", "商业航天"), ("军工", ["军工", "地缘风险", "商业航天"])),
    (("黄金", "贵金属"), ("黄金", ["黄金", "通胀预期"])),
    (("电网", "电力", "储能", "特高压"), ("电网", ["AI算力", "电力需求", "电网设备"])),
    (("有色", "铜", "铝", "黄金股"), ("有色", ["铜铝", "顺周期"])),
    (("医药", "医疗", "创新药"), ("医药", ["医药", "老龄化"])),
    (("消费", "食品", "饮料", "家电", "零售", "消费龙头"), ("消费", ["内需", "消费修复"])),
    (("红利", "高股息", "股息", "银行", "公用事业"), ("高股息", ["高股息", "防守"])),
    (("能源", "原油", "煤炭", "油气", "能化", "化工"), ("能源", ["原油", "能源安全", "通胀预期"])),
    (("沪深300", "中证a500", "a500", "中证500", "上证50", "宽基"), ("宽基", ["宽基", "大盘蓝筹", "内需"])),
]

OVERSEAS_KEYWORDS = ("nasdaq", "纳斯达克", "标普", "sp500", "s&p", "港股", "美股", "hong kong", "qdii", "海外")


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


def infer_fund_sector(text: str, sector_hint: str = "") -> tuple[str, list[str]]:
    lowered = _theme_detection_text(" ".join([str(sector_hint or ""), str(text or "")])).lower()
    for keywords, payload in FUND_TAXONOMY_RULES:
        if any(keyword.lower() in lowered for keyword in keywords):
            return payload[0], list(payload[1])
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
    }


def taxonomy_rows(taxonomy: Mapping[str, Any]) -> list[list[str]]:
    payload = dict(taxonomy or {})
    return [
        ["产品形态", str(payload.get("product_form", "—"))],
        ["载体角色", str(payload.get("vehicle_role", "—"))],
        ["管理方式", str(payload.get("management_style", "—"))],
        ["暴露类型", str(payload.get("exposure_scope", "—"))],
        ["主方向", str(payload.get("sector", "—"))],
        ["份额类别", str(payload.get("share_class", "—"))],
    ]


def taxonomy_from_analysis(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    fund_profile = dict(analysis.get("fund_profile") or {})
    style = dict(fund_profile.get("style") or {})
    taxonomy = dict(style.get("taxonomy") or {})
    if taxonomy:
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
