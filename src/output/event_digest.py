"""Shared event digestion contract for client-facing research reports."""

from __future__ import annotations

from datetime import datetime
import re
from typing import Any, Dict, List, Mapping, Sequence


_STATUS_VALUES = {"待补充", "待复核", "已消化"}
_IMPORTANCE_LABELS = {
    "high": "高",
    "medium": "中",
    "low": "低",
}
_LAYER_BASE_SCORES = {
    "财报": 100,
    "公告": 92,
    "政策": 88,
    "行业主题事件": 72,
    "新闻": 58,
}
_IMPACT_AXIS_ORDER = {
    "盈利": 0,
    "估值": 1,
    "景气": 2,
    "资金偏好": 3,
}
_NEGATIVE_TOKENS = (
    "减持",
    "处罚",
    "诉讼",
    "问询",
    "终止",
    "下修",
    "下调",
    "停产",
    "召回",
    "减值",
    "风险",
    "亏损",
    "失速",
    "违约",
    "预亏",
)
_POLICY_TOKENS = (
    "政策",
    "通知",
    "意见",
    "方案",
    "规划",
    "行动计划",
    "国常会",
    "国务院",
    "发改委",
    "工信部",
    "证监会",
    "财政部",
    "央行",
    "医保局",
)
_EARNINGS_TOKENS = (
    "财报",
    "业绩",
    "业绩预告",
    "业绩快报",
    "年报",
    "中报",
    "季报",
    "一季报",
    "半年报",
    "三季报",
    "指引",
    "盈利",
    "收入",
    "净利",
)
_ANNOUNCEMENT_TOKENS = (
    "公告",
    "回购",
    "增持",
    "减持",
    "解禁",
    "中标",
    "订单",
    "签约",
    "定增",
    "并购",
    "重组",
    "分红",
    "投产",
    "扩产",
    "停牌",
    "复牌",
)
_THEME_EVENT_TOKENS = (
    "产业链",
    "主题",
    "板块",
    "大会",
    "展会",
    "发布会",
    "招标",
    "装机",
    "价格",
    "景气",
    "供给",
    "排产",
    "数据中心",
    "出货",
)
_FIRST_PARTY_SOURCE_TOKENS = (
    "公司公告",
    "巨潮资讯",
    "CNINFO",
    "上交所",
    "深交所",
    "SSE",
    "SZSE",
    "Investor Relations",
    "SEC",
    "HKEX",
    "hkexnews",
)
_DIRECT_MEDIA_SOURCE_TOKENS = (
    "Reuters",
    "Bloomberg",
    "财联社",
    "证券时报",
    "上海证券报",
    "中国证券报",
    "证券日报",
)
_STRUCTURED_DISCLOSURE_LAYERS = {
    "结构化事件",
    "龙头公告/业绩",
    "个股公告/事件",
    "负面事件",
    "政策催化",
    "财报",
}
_STRUCTURED_DISCLOSURE_CATEGORY_TOKENS = (
    "structured",
    "announcement",
    "disclosure",
)
_SEARCH_FALLBACK_CATEGORY_TOKENS = (
    "search",
    "rss",
    "feed",
)
_INTELLIGENCE_ATTRIBUTE_ORDER = {
    "首次跟踪": 0,
    "新鲜情报": 1,
    "旧闻回放": 2,
    "一手直连": 3,
    "结构化披露": 4,
    "官网/IR回退": 5,
    "官方搜索回退": 6,
    "媒体直连": 7,
    "搜索回退": 8,
    "主题级情报": 9,
}
_ANNOUNCEMENT_TYPE_TOKENS = (
    ("中标/订单", ("中标", "订单", "签约", "合同", "拿单")),
    ("投资者关系/路演纪要", ("投资者关系", "活动记录表", "调研纪要", "路演纪要", "业绩说明会", "交流纪要", "电话会纪要", "互动易", "互动平台", "投资者问答", "e互动")),
    ("问询/回复函", ("问询函", "回复函", "问询回复", "监管函", "关注函", "工作函")),
    ("产品/新品", ("新品", "新产品", "新一代", "发布", "发布会", "样机", "型号", "迭代")),
    ("扩产/投产", ("扩产", "投产", "量产", "开工", "产线", "capex")),
    ("回购/增持", ("回购", "增持", "员工持股", "股权激励")),
    ("减持/解禁", ("减持", "解禁", "清仓")),
    ("并购/重组", ("并购", "收购", "重组", "资产注入")),
    ("分红/回报", ("分红", "派息", "股息", "特别分红")),
    ("融资/定增", ("定增", "配股", "可转债", "融资", "募资")),
)
_POLICY_IMPACT_TIER_TOKENS = (
    ("财政支持/名单落地", ("补贴", "专项资金", "税收优惠", "名单", "目录", "配套资金", "首批", "入围")),
    ("价格机制/收费调整", ("价格机制", "电价", "容量电价", "收费", "费率", "输配电价", "收益口径", "定价机制")),
    ("直接执行", ("落地", "实施", "执行", "批复", "采购", "目录", "名单", "下达", "补贴金额", "配套资金")),
    ("配套细则", ("细则", "办法", "指引", "试点", "规则", "征求意见", "配套")),
    ("方向表态", ("意见", "规划", "行动计划", "会议", "座谈会", "国常会")),
)
_EARNINGS_DETAIL_TOKENS = (
    ("存货/减值压力", ("存货", "库存压力", "存货跌价", "减值准备", "资产减值", "跌价准备", "计提减值")),
    ("毛利率/费用率改善", ("毛利率提升", "毛利改善", "费用率下降", "费用优化", "净利率提升", "盈利质量改善", "经营杠杆改善")),
    ("现金流/合同负债改善", ("经营现金流", "自由现金流", "现金流改善", "合同负债", "预收款", "回款改善", "回款提速")),
    ("盈利/指引承压", ("下修", "下调", "预亏", "亏损", "减值", "利润下滑", "毛利承压", "指引下调")),
    ("盈利/指引上修", ("上修", "超预期", "高增长", "利润增长", "盈利改善", "指引上调")),
    ("资本开支/扩产", ("资本开支", "capex", "扩产", "扩建", "产能建设")),
    ("回购/分红", ("回购", "分红", "派息", "特别分红", "股东回报")),
    ("收入/订单验证", ("收入", "营收", "订单", "出货", "销量")),
)
_THEME_EVENT_DETAIL_TOKENS = (
    ("价格/排产验证", ("涨价", "价格", "排产", "装机", "出货", "补库", "去库", "开工", "产销", "库存拐点")),
    ("海外映射/链式催化", ("海外映射", "英伟达", "nvidia", "苹果", "apple", "特斯拉", "tesla", "amd", "海外龙头", "海外发布")),
    ("大会/产品催化", ("大会", "展会", "发布会", "新品", "模型")),
    ("情绪热度", ("热度", "情绪升温", "讨论度", "热搜", "刷屏", "涨停潮", "爆发", "发酵", "拥挤")),
    ("产业链映射", ("产业链", "供应链", "映射", "主题")),
)
_IMPACT_AXIS_TOKENS = (
    ("盈利", ("盈利", "利润", "净利", "收入", "营收", "毛利", "eps", "业绩", "指引", "订单", "中标", "合同", "减值", "亏损")),
    ("估值", ("估值", "重估", "折现", "市盈率", "pe", "pb", "回购", "分红", "利率", "风险偏好")),
    ("景气", ("景气", "需求", "供给", "库存", "排产", "开工", "装机", "出货", "价格", "涨价", "降价", "产能")),
    ("资金偏好", ("资金", "风险偏好", "北向", "南向", "拥挤", "抱团", "情绪", "主题", "避险", "风格", "高股息", "成长")),
)
_DIAGNOSTIC_INTELLIGENCE_TOKENS = (
    "内部覆盖率摘要",
    "覆盖率摘要",
    "新增情报 0 条",
    "主题/行业情报 0 条",
    "当前没有抓到高置信直连证据",
    "当前可前置的一手情报有限",
    "当前前置证据以结构化披露和主题线索为主",
    "当前可前置的外部情报仍偏少",
    "待 AI 联网复核",
    "判断更多参考结构化事件和行业线索",
    "个股新增直接情报 0 条",
    "主题/行业情报 0 条",
)
_DIAGNOSTIC_FACTOR_TOKENS = (
    "新增情报 0 条",
    "相关头条 0 条",
    "未命中直接",
    "未来 14 日未命中",
    "未来 30 日未命中",
    "近 7 日未命中",
    "近 30 日未命中",
    "覆盖源 0 个",
    "新鲜情报 0 条",
    "待 AI 联网复核",
    "前瞻催化窗口暂不突出",
    "直接政策情报偏弱",
    "直接龙头公告/业绩情报偏弱",
    "直接海外映射情报偏弱",
    "情报覆盖偏窄",
    "当前前置证据以结构化披露和主题线索为主",
    "当前可前置的外部情报仍偏少",
    "个股新增直接情报 0 条",
    "主题/行业情报 0 条",
)


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _markdown_link(label: Any, link: Any) -> str:
    text = _safe_text(label)
    url = _safe_text(link)
    if text and url:
        return f"[{text}]({url})"
    return text or url


def _compact_event_label(value: Any) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    if "；" in text:
        text = text.split("；", 1)[0]
    for prefix in ("主题事件：", "财报摘要：", "公告类型：", "政策影响层："):
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    return text.strip("：: ").strip()


def _compact_changed_what_text(changed_what: Any, *, impact_summary: Any = "", thesis_scope: Any = "") -> str:
    text = _safe_text(changed_what)
    if not text:
        return ""
    if "；结论：" not in text and not text.startswith("主题事件：") and len(text) <= 88 and text.count("；") <= 1:
        return text
    impact = _safe_text(impact_summary) or "研究判断"
    scope = _safe_text(thesis_scope) or "辅助线索"
    return f"当前更直接影响 `{impact}`，性质上更像 `{scope}`。"


def _compact_priority_reason_text(
    importance_reason: Any,
    *,
    lead_detail: Any = "",
    thesis_scope: Any = "",
) -> str:
    reason = compact_importance_reason(importance_reason)
    if not reason:
        return ""
    if _safe_text(thesis_scope) == "一次性噪音":
        return "先按辅助线索看，不单独升级动作。"
    if _safe_text(lead_detail).startswith("主题事件：") and "先不升级" in reason:
        return "先别把这条线直接升级成动作。"
    return reason


def _compact_homepage_evidence_text(value: Any) -> str:
    text = _safe_text(value)
    if not text:
        return ""
    for prefix in ("指数成分权重：", "指数技术面：", "指数主链：", "行业主题事件："):
        if text.startswith(prefix):
            text = text[len(prefix) :]
            break
    parts = [part.strip() for part in text.split("；") if part.strip()]
    if not parts:
        return ""
    compact_parts = parts[:2]
    compact = "；".join(compact_parts)
    if compact == text:
        return compact
    return f"{compact}。"


def _structured_disclosure_fallback_link(symbol: Any) -> str:
    code = _safe_text(symbol)
    if not code:
        return ""
    return f"https://www.cninfo.com.cn/new/disclosure/detail?stockCode={code}"


def effective_intelligence_link(item: Mapping[str, Any], *, symbol: Any = "") -> str:
    row = dict(item or {})
    direct = _safe_text(row.get("link"))
    if direct:
        return direct
    configured_source = _safe_text(row.get("configured_source"))
    source = _safe_text(row.get("source"))
    source_note = _safe_text(row.get("source_note"))
    configured_source_lower = configured_source.lower()
    source_lower = source.lower()
    title_lower = _safe_text(row.get("title")).lower()
    lead_detail_lower = _safe_text(row.get("lead_detail")).lower()
    if "irm_qa_sh" in configured_source_lower or "上证e互动" in title_lower:
        return "https://sns.sseinfo.com/"
    if "irm_qa_sz" in configured_source_lower or "互动易" in title_lower or "e互动" in title_lower:
        return "https://irm.cninfo.com.cn/"
    if (
        configured_source.startswith("Tushare::")
        or configured_source_lower.startswith("tushare ")
        or configured_source_lower == "tushare"
        or source_lower.startswith("tushare ")
        or source_lower == "tushare"
        or source_note == "structured_disclosure"
    ):
        return _structured_disclosure_fallback_link(symbol)
    if any(
        token in " ".join(part for part in (source_note, configured_source_lower, source_lower, title_lower, lead_detail_lower) if part)
        for token in (
            "结构化事件",
            "结构化披露",
            "公告类型",
            "财报摘要",
            "披露",
            "分红",
            "业绩预告",
            "业绩快报",
            "股东",
            "问询",
            "公告",
        )
    ):
        return _structured_disclosure_fallback_link(symbol)
    return ""


def _parse_datetime(value: Any) -> datetime | None:
    text = _safe_text(value)
    if not text:
        return None
    normalized = text.replace("Z", "+00:00")
    for candidate in (normalized, text):
        try:
            stamp = datetime.fromisoformat(candidate)
        except ValueError:
            stamp = None
        if stamp is not None:
            if stamp.tzinfo is not None:
                stamp = stamp.astimezone().replace(tzinfo=None)
            return stamp
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None


def _flatten_text(*values: Any) -> str:
    parts: List[str] = []
    for value in values:
        if value is None:
            continue
        if isinstance(value, Mapping):
            parts.append(_flatten_text(*value.values()))
            continue
        if isinstance(value, (list, tuple, set)):
            parts.append(_flatten_text(*list(value)))
            continue
        text = _safe_text(value)
        if text:
            parts.append(text)
    return " | ".join(item for item in parts if item)


def _parse_event_date(value: Any) -> datetime | None:
    text = _safe_text(value)
    if not text:
        return None
    normalized = text.replace("/", "-")
    for candidate in (normalized[:19], normalized[:10], normalized):
        try:
            return datetime.fromisoformat(candidate)
        except ValueError:
            continue
    try:
        return datetime.fromisoformat(text.replace("Z", "+00:00").replace("/", "-"))
    except ValueError:
        return None


def _item_event_stamp(item: Mapping[str, Any]) -> datetime | None:
    return _parse_datetime(item.get("date") or item.get("published_at") or item.get("as_of"))


def _contains_any(text: str, tokens: Sequence[str]) -> bool:
    blob = _safe_text(text).lower()
    return any(_safe_text(token).lower() in blob for token in tokens)


def _source_directness_rank(source: Any) -> int:
    text = _safe_text(source)
    if not text:
        return 0
    if _contains_any(text, _FIRST_PARTY_SOURCE_TOKENS):
        return 2
    if _contains_any(text, _DIRECT_MEDIA_SOURCE_TOKENS):
        return 1
    return 0


def _is_official_search_item(item: Mapping[str, Any]) -> bool:
    note_blob = _flatten_text(
        item.get("source_note"),
        item.get("configured_source"),
        item.get("source"),
        item.get("category"),
    ).lower()
    return "official_search_fallback" in note_blob or "official_site_search" in note_blob


def _item_source_directness_rank(item: Mapping[str, Any]) -> int:
    if _is_official_search_item(item):
        return 1
    return _source_directness_rank(_flatten_text(item.get("source"), item.get("configured_source")))


def _source_tier_rank(item: Mapping[str, Any]) -> int:
    if _is_search_fallback_item(item) and not _is_official_search_item(item):
        return 0
    if _is_official_search_item(item):
        return 2
    source_rank = int(item.get("source_directness_rank", 0) or 0)
    if source_rank <= 0:
        source_rank = _item_source_directness_rank(item)
    if source_rank >= 2:
        return 3
    if _is_structured_disclosure(item):
        return 2
    if source_rank == 1:
        return 1
    return 0


def _intelligence_lane_rank(item: Mapping[str, Any]) -> int:
    source_rank = int(item.get("source_directness_rank", 0) or 0)
    if source_rank <= 0:
        source_rank = _item_source_directness_rank(item)
    structured = _is_structured_disclosure(item)
    official_search = _is_official_search_item(item)
    if source_rank >= 2 and structured:
        return 5
    if source_rank >= 2:
        return 4
    if structured:
        return 3
    if official_search:
        return 2
    if source_rank == 1:
        return 1
    return 0


def _freshness_rank(item: Mapping[str, Any]) -> int:
    freshness = _safe_text(item.get("freshness_bucket")).lower()
    if freshness in {"fresh", "new"}:
        return 2
    if freshness == "recent":
        return 1
    if freshness == "stale":
        return 0
    age_days = item.get("age_days")
    try:
        age_value = float(age_days)
    except (TypeError, ValueError):
        age_value = None
    if age_value is not None:
        if age_value <= 3:
            return 2
        if age_value <= 7:
            return 1
    return 0


def _ordered_intelligence_labels(labels: Sequence[str]) -> List[str]:
    seen: set[str] = set()
    ordered: List[str] = []
    for label in sorted(
        (_safe_text(item) for item in labels if _safe_text(item)),
        key=lambda item: (_INTELLIGENCE_ATTRIBUTE_ORDER.get(item, 99), item),
    ):
        if label in seen:
            continue
        seen.add(label)
        ordered.append(label)
    return ordered


def _is_structured_disclosure(item: Mapping[str, Any]) -> bool:
    layer = _safe_text(item.get("raw_layer")) or _safe_text(item.get("layer"))
    category = _safe_text(item.get("category")).lower()
    configured_source = _safe_text(item.get("configured_source")).lower()
    if layer in _STRUCTURED_DISCLOSURE_LAYERS:
        return True
    if any(token in category for token in _STRUCTURED_DISCLOSURE_CATEGORY_TOKENS):
        return True
    if configured_source.startswith("tushare::"):
        return True
    if _item_source_directness_rank(item) >= 1 or _is_official_search_item(item):
        blob = _flatten_text(
            layer,
            item.get("lead_detail"),
            item.get("title"),
            item.get("signal"),
            category,
            configured_source,
        )
        if (
            _contains_any(blob, _EARNINGS_TOKENS)
            or _contains_any(blob, _ANNOUNCEMENT_TOKENS)
            or _contains_any(blob, _POLICY_TOKENS)
        ):
            return True
    return False


def _is_search_fallback_item(item: Mapping[str, Any]) -> bool:
    category = _safe_text(item.get("category")).lower()
    configured_source = _safe_text(item.get("configured_source")).lower()
    note_blob = _flatten_text(
        category,
        configured_source,
        item.get("source_note"),
        item.get("note"),
        item.get("search_query"),
    ).lower()
    return any(token in category for token in _SEARCH_FALLBACK_CATEGORY_TOKENS) or any(
        token in note_blob for token in _SEARCH_FALLBACK_CATEGORY_TOKENS
    )


def format_intelligence_attributes(labels: Sequence[str]) -> str:
    return " / ".join(_ordered_intelligence_labels(labels))


def intelligence_source_lane(labels: Sequence[str]) -> str:
    ordered = _ordered_intelligence_labels(labels)
    if "一手直连" in ordered and "结构化披露" in ordered:
        return "官方直连 / 结构化披露"
    if "一手直连" in ordered:
        return "官方直连"
    if "结构化披露" in ordered:
        return "结构化披露"
    if "官网/IR回退" in ordered:
        return "官网/IR回退"
    if "官方搜索回退" in ordered:
        return "官方搜索回退"
    if "媒体直连" in ordered:
        return "媒体直连"
    if "搜索回退" in ordered:
        return "搜索回退"
    return ""


def intelligence_attribute_labels(
    item: Mapping[str, Any],
    *,
    as_of: Any = None,
    first_tracking: bool = False,
    previous_reviewed_at: Any = None,
) -> List[str]:
    labels: List[str] = []
    if first_tracking:
        labels.append("首次跟踪")
    review_stamp = _parse_datetime(previous_reviewed_at) if previous_reviewed_at else None
    event_stamp = _item_event_stamp(item)
    previously_reviewed = review_stamp is not None and event_stamp is not None and event_stamp <= review_stamp
    if previously_reviewed:
        labels.append("旧闻回放")
    freshness_rank = int(item.get("freshness_rank", 0) or 0)
    explicit_freshness = "freshness_bucket" in item or "age_days" in item
    if freshness_rank <= 0:
        freshness_rank = _freshness_rank(item)
    if not previously_reviewed and freshness_rank >= 2:
        labels.append("新鲜情报")
    elif not previously_reviewed and explicit_freshness and freshness_rank <= 0:
        labels.append("旧闻回放")
    elif not previously_reviewed:
        as_of_stamp = _parse_datetime(as_of) if as_of else None
        if as_of_stamp is not None and event_stamp is not None and event_stamp <= as_of_stamp:
            age_days = (as_of_stamp.date() - event_stamp.date()).days
            if age_days <= 3:
                labels.append("新鲜情报")
            elif age_days >= 8:
                labels.append("旧闻回放")
    source_rank = int(item.get("source_directness_rank", 0) or 0)
    if source_rank <= 0:
        source_rank = _item_source_directness_rank(item)
    if source_rank >= 2 and not _is_official_search_item(item):
        labels.append("一手直连")
    if _safe_text(item.get("source_note")) == "official_site_search":
        labels.append("官网/IR回退")
    elif _safe_text(item.get("source_note")) == "official_search_fallback":
        labels.append("官方搜索回退")
    if _is_structured_disclosure(item):
        labels.append("结构化披露")
    if source_rank == 1 and not _is_official_search_item(item):
        labels.append("媒体直连")
    if _is_search_fallback_item(item):
        labels.append("搜索回退")
    theme_blob = _flatten_text(item.get("layer"), item.get("category"), item.get("lead_detail"))
    if "主题" in theme_blob or "行业主题事件" in theme_blob:
        labels.append("主题级情报")
    return _ordered_intelligence_labels(labels)


def _since_last_review_rank(item: Mapping[str, Any], previous_reviewed_at: Any = None) -> int:
    previous_stamp = _parse_datetime(previous_reviewed_at) if previous_reviewed_at else None
    event_stamp = _item_event_stamp(item)
    if previous_stamp is None or event_stamp is None:
        return 0
    return 1 if event_stamp > previous_stamp else -1


def _normalize_event_layer(layer: Any = "", title: Any = "", signal: Any = "", *, prefer_theme_event: bool = False) -> str:
    layer_text = _safe_text(layer)
    content_blob = _flatten_text(title, signal)
    blob = _flatten_text(layer, title, signal)
    if _contains_any(content_blob, _EARNINGS_TOKENS):
        return "财报"
    if "政策" in layer_text or _contains_any(content_blob, _POLICY_TOKENS):
        return "政策"
    if layer_text in {"结构化事件", "个股公告/事件", "负面事件", "龙头公告/业绩"} or _contains_any(content_blob, _ANNOUNCEMENT_TOKENS):
        return "公告"
    if prefer_theme_event or layer_text in {"海外映射", "产品/跟踪方向催化"} or _contains_any(content_blob, _THEME_EVENT_TOKENS):
        return "行业主题事件"
    if _contains_any(blob, _EARNINGS_TOKENS):
        return "财报"
    if "政策" in layer_text or _contains_any(blob, _POLICY_TOKENS):
        return "政策"
    if layer_text in {"结构化事件", "个股公告/事件", "负面事件"} or _contains_any(blob, _ANNOUNCEMENT_TOKENS):
        return "公告"
    if prefer_theme_event or layer_text in {"海外映射", "产品/跟踪方向催化"} or _contains_any(blob, _THEME_EVENT_TOKENS):
        return "行业主题事件"
    return "新闻"


def _importance_bucket(score: int) -> str:
    if score >= 90:
        return "high"
    if score >= 68:
        return "medium"
    return "low"


def _first_match_label(text: str, groups: Sequence[tuple[str, Sequence[str]]], *, fallback: str) -> str:
    for label, tokens in groups:
        if _contains_any(text, tokens):
            return label
    return fallback


def _announcement_type(text: str) -> str:
    return _first_match_label(text, _ANNOUNCEMENT_TYPE_TOKENS, fallback="一般公告")


def _policy_impact_tier(text: str) -> str:
    return _first_match_label(text, _POLICY_IMPACT_TIER_TOKENS, fallback="方向表态")


def _earnings_detail(text: str, *, negative: bool = False) -> str:
    label = _first_match_label(text, _EARNINGS_DETAIL_TOKENS, fallback="盈利/指引")
    if label == "盈利/指引" and negative:
        return "盈利/指引承压"
    return label


def _theme_event_detail(text: str) -> str:
    return _first_match_label(text, _THEME_EVENT_DETAIL_TOKENS, fallback="主题热度/映射")


def _lead_detail(layer: str, text: str, *, negative: bool = False) -> str:
    if layer == "财报":
        return f"财报摘要：{_earnings_detail(text, negative=negative)}"
    if layer == "公告":
        return f"公告类型：{_announcement_type(text)}"
    if layer == "政策":
        return f"政策影响层：{_policy_impact_tier(text)}"
    if layer == "行业主题事件":
        return f"主题事件：{_theme_event_detail(text)}"
    return "信息环境：新闻/舆情脉冲"


def _default_impact_axes(layer: str, detail: str) -> List[str]:
    if layer == "财报":
        if "毛利率/费用率改善" in detail:
            return ["盈利", "估值"]
        if "现金流/合同负债改善" in detail:
            return ["盈利", "景气"]
        if "存货/减值压力" in detail:
            return ["盈利", "景气"]
        if "资本开支/扩产" in detail:
            return ["景气", "盈利"]
        if "回购/分红" in detail:
            return ["估值", "资金偏好"]
        return ["盈利", "估值"]
    if layer == "公告":
        if "中标/订单" in detail or "扩产/投产" in detail:
            return ["盈利", "景气"]
        if "投资者关系/路演纪要" in detail:
            return ["景气", "资金偏好"]
        if "问询/回复函" in detail:
            return ["盈利", "估值"]
        if "产品/新品" in detail:
            return ["景气", "资金偏好"]
        if "并购/重组" in detail:
            return ["盈利", "估值"]
        if "融资/定增" in detail:
            return ["估值", "资金偏好"]
        if "回购/增持" in detail or "分红/回报" in detail:
            return ["估值", "资金偏好"]
        if "减持/解禁" in detail or "融资/定增" in detail:
            return ["资金偏好", "估值"]
        return ["盈利", "估值"]
    if layer == "政策":
        if "财政支持/名单落地" in detail:
            return ["盈利", "景气"]
        if "价格机制/收费调整" in detail:
            return ["盈利", "估值"]
        if "直接执行" in detail:
            return ["景气", "盈利"]
        if "配套细则" in detail:
            return ["景气", "估值"]
        return ["资金偏好", "景气"]
    if layer == "行业主题事件":
        if "价格/排产验证" in detail:
            return ["景气", "盈利"]
        if "大会/产品催化" in detail:
            return ["资金偏好", "景气"]
        if "海外映射/链式催化" in detail:
            return ["资金偏好", "景气"]
        if "情绪热度" in detail:
            return ["资金偏好"]
        return ["资金偏好", "景气"]
    return ["资金偏好"]


def _impact_axes(layer: str, text: str, detail: str) -> List[str]:
    if layer == "财报" and "毛利率/费用率改善" in detail:
        return ["盈利", "估值"]
    if layer == "财报" and "现金流/合同负债改善" in detail:
        return ["盈利", "景气"]
    if layer == "财报" and "存货/减值压力" in detail:
        return ["盈利", "景气"]
    if layer == "财报" and "回购/分红" in detail:
        return ["估值", "资金偏好"]
    if layer == "财报" and "资本开支/扩产" in detail:
        return ["景气", "盈利"]
    if layer == "政策" and "财政支持/名单落地" in detail:
        return ["盈利", "景气"]
    if layer == "政策" and "价格机制/收费调整" in detail:
        return ["盈利", "估值"]
    if layer == "行业主题事件" and "价格/排产验证" in detail:
        return ["景气", "盈利"]
    if layer == "行业主题事件" and "大会/产品催化" in detail:
        return ["资金偏好", "景气"]
    if layer == "行业主题事件" and "海外映射/链式催化" in detail:
        return ["资金偏好", "景气"]
    if layer == "行业主题事件" and "情绪热度" in detail:
        return ["资金偏好"]
    counts: Dict[str, int] = {}
    for axis, tokens in _IMPACT_AXIS_TOKENS:
        hits = sum(1 for token in tokens if _contains_any(text, [token]))
        if hits > 0:
            counts[axis] = hits
    ranked = sorted(
        counts.items(),
        key=lambda item: (-int(item[1]), _IMPACT_AXIS_ORDER.get(item[0], 99)),
    )
    axes = [axis for axis, _ in ranked[:3]]
    defaults = _default_impact_axes(layer, detail)
    if not axes:
        axes = list(defaults)
    elif len(axes) < 2:
        for axis in defaults:
            if axis not in axes:
                axes.append(axis)
            if len(axes) >= 2:
                break
    return axes[:3]


def _impact_summary(axes: Sequence[str]) -> str:
    ordered = sorted(
        {_safe_text(item) for item in list(axes or []) if _safe_text(item)},
        key=lambda item: _IMPACT_AXIS_ORDER.get(item, 99),
    )
    return " / ".join(ordered)


def _signal_strength_label(value: Any) -> str:
    text = _safe_text(value).lower()
    return {
        "high": "强",
        "medium": "中",
        "low": "弱",
        "高": "强",
        "中": "中",
        "低": "弱",
        "强": "强",
        "弱": "弱",
    }.get(text, _safe_text(value) or "中")


def _signal_conclusion_text(
    *,
    status: str = "",
    lead_detail: str = "",
    impact_summary: str = "",
    thesis_scope: str = "",
    negative: bool = False,
    explicit: Any = "",
) -> str:
    if thesis_scope == "历史基线":
        return "中性，当前更多是历史基线，不把它直接当成新增催化。"
    explicit_text = _safe_text(explicit)
    if explicit_text:
        return explicit_text
    if status == "待复核":
        return "待复核，先补更直接的直连证据再定级。"
    if thesis_scope == "thesis变化":
        prefix = "偏利空" if negative else "偏利多"
        if impact_summary:
            return f"{prefix}，已开始改写 `{impact_summary}` 这层。"
        return f"{prefix}，已开始改写当前研究重点。"
    if thesis_scope == "待确认":
        prefix = "偏利空" if negative else "中性偏多"
        if impact_summary:
            return f"{prefix}，先看 `{impact_summary}` 能否继续拿到确认。"
        return f"{prefix}，先看这条线能否继续拿到确认。"
    if thesis_scope == "一次性噪音":
        prefix = "偏利空" if negative else "中性"
        return f"{prefix}，先别把它单独升级成动作。"
    if status == "待补充":
        return "中性，先补更直接的公司/产品级情报。"
    if negative:
        return "偏利空，先看风险会不会继续扩散。"
    if impact_summary:
        return f"中性偏多，先看 `{impact_summary}` 能否继续拿到确认。"
    if lead_detail:
        return f"中性偏多，先看 `{lead_detail}` 能否继续拿到确认。"
    return "中性，先补更多直接情报。"


def _thesis_scope(layer: str, score: int, axes: Sequence[str], detail: str, *, negative: bool = False, status: str = "") -> str:
    if status == "待复核":
        return "待确认"

    axis_set = {_safe_text(item) for item in list(axes or []) if _safe_text(item)}
    if layer == "财报" and ("资本开支/扩产" in detail or "回购/分红" in detail or "现金流/合同负债改善" in detail):
        return "待确认" if score >= 68 else "一次性噪音"
    if layer == "公告" and (
        ("产品/新品" in detail and "盈利" not in axis_set)
        or "投资者关系/路演纪要" in detail
        or "问询/回复函" in detail
        or "融资/定增" in detail
        or "并购/重组" in detail
        or "减持/解禁" in detail
    ):
        return "待确认" if score >= 68 else "一次性噪音"
    if layer == "政策" and ("价格机制/收费调整" in detail or "配套细则" in detail or "方向表态" in detail):
        return "待确认" if score >= 68 else "一次性噪音"
    if layer == "行业主题事件" and (
        "大会/产品催化" in detail
        or "海外映射/链式催化" in detail
        or "情绪热度" in detail
    ):
        return "待确认" if score >= 68 else "一次性噪音"
    if layer in {"财报", "公告", "政策"} and score >= 76:
        return "thesis变化"
    if layer == "行业主题事件" and score >= 84 and ("景气" in axis_set or "盈利" in axis_set):
        return "thesis变化" if status == "已消化" else "待确认"
    if negative and score >= 76:
        return "thesis变化"
    if layer == "新闻" or score < 68:
        return "一次性噪音"
    if "政策影响层：方向表态" in detail:
        return "待确认"
    return "一次性噪音"


def _signal_strength_for_scope(value: Any, *, thesis_scope: str = "", status: str = "") -> str:
    if thesis_scope == "历史基线" and status != "待复核":
        return "中"
    return _signal_strength_label(value)


def _importance_score(
    layer: str,
    *,
    event_date: Any = "",
    as_of: Any = "",
    has_direct_source: bool = False,
    text: str = "",
    detail: str = "",
    axes: Sequence[str] | None = None,
    thesis_scope: str = "",
) -> int:
    score = _LAYER_BASE_SCORES.get(layer, 50)
    if has_direct_source:
        score += 6
    if _contains_any(text, _NEGATIVE_TOKENS):
        score += 4
    if layer == "财报":
        if "上修" in detail or "承压" in detail:
            score += 5
        if "毛利率/费用率改善" in detail:
            score += 3
        if "现金流/合同负债改善" in detail:
            score -= 34
        if "存货/减值压力" in detail:
            score += 4
        if "资本开支/扩产" in detail:
            score -= 35
        elif "回购/分红" in detail:
            score -= 30
    elif layer == "公告":
        if "中标/订单" in detail or "扩产/投产" in detail or "并购/重组" in detail:
            score += 4
        if "投资者关系/路演纪要" in detail:
            score -= 12
        if "问询/回复函" in detail:
            score -= 8
        if "产品/新品" in detail:
            score -= 20
        if "融资/定增" in detail:
            score -= 24
        if "并购/重组" in detail:
            score -= 15
        if "减持/解禁" in detail:
            score -= 18
    elif layer == "政策":
        if "财政支持/名单落地" in detail:
            score += 6
        elif "价格机制/收费调整" in detail:
            score -= 10
        if "直接执行" in detail:
            score += 5
        elif "配套细则" in detail:
            score -= 18
        elif "方向表态" in detail:
            score -= 5
    elif layer == "行业主题事件":
        if "价格/排产验证" in detail:
            score += 4
        elif "大会/产品催化" in detail:
            score -= 2
        elif "海外映射/链式催化" in detail:
            score -= 8
        elif "情绪热度" in detail:
            score -= 12
        elif "主题热度/映射" in detail:
            score -= 4

    axes_set = {_safe_text(item) for item in list(axes or []) if _safe_text(item)}
    if len(axes_set) >= 2:
        score += 3
    if "盈利" in axes_set and "景气" in axes_set:
        score += 3
    if thesis_scope == "thesis变化":
        score += 5
    elif thesis_scope == "一次性噪音":
        score -= 4

    event_dt = _parse_event_date(event_date)
    as_of_dt = _parse_event_date(as_of)
    if event_dt and as_of_dt:
        age_days = abs((as_of_dt.date() - event_dt.date()).days)
        if age_days <= 3:
            score += 8
        elif age_days <= 7:
            score += 4
        elif age_days >= 20:
            score -= 8
    return max(min(score, 100), 0)


def _event_title_with_source(item: Mapping[str, Any]) -> str:
    title = _safe_text(item.get("title")) or _safe_text(item.get("signal"))
    source = _safe_text(item.get("source"))
    date = _safe_text(item.get("date"))
    suffix = " / ".join(part for part in (source, date) if part)
    if title and suffix:
        return f"{title}（{suffix}）"
    return title or suffix


def _is_placeholder_intelligence_title(value: Any) -> bool:
    title = _safe_text(value)
    if not title:
        return True
    prefixes = (
        "未命中",
        "近 7 日未命中",
        "近 30 日未命中",
        "未来 14 日未命中",
        "未来 30 日未命中",
        "未识别到",
    )
    if title.startswith(prefixes):
        return True
    if "暂不计分" in title:
        return True
    if "新增情报 0 条" in title:
        return True
    if "新增直接情报 0 条" in title:
        return True
    if "主题/行业情报 0 条" in title:
        return True
    return False


def _extract_factor_age_days(text: Any) -> float | None:
    blob = _safe_text(text)
    if not blob:
        return None
    match = re.search(r"(?:事件距今|距今)\s*(\d+)\s*天", blob)
    if not match:
        return None
    try:
        return float(match.group(1))
    except (TypeError, ValueError):
        return None


def _is_diagnostic_intelligence_row(item: Mapping[str, Any] | str) -> bool:
    if isinstance(item, str):
        blob = _safe_text(item)
    else:
        row = dict(item or {})
        blob = " ".join(
            part
            for part in (
                _safe_text(row.get("title")),
                _safe_text(row.get("source")),
                _safe_text(row.get("note")),
                _safe_text(row.get("lead_detail")),
            )
            if part
        )
    return any(token in blob for token in _DIAGNOSTIC_INTELLIGENCE_TOKENS)


_WORKFLOW_EVENT_TOKENS = (
    "A股盘前检查",
    "收盘复核",
    "盘后复核",
    "复核日内强弱",
    "检查 watchlist",
    "下个交易日 09:00",
    "美股开盘前观察",
    "上午验证",
    "下午验证",
    "明日验证",
)


def _is_workflow_event_row(item: Mapping[str, Any] | str) -> bool:
    if isinstance(item, str):
        blob = _safe_text(item)
    else:
        row = dict(item or {})
        blob = " ".join(
            part
            for part in (
                _safe_text(row.get("title")),
                _safe_text(row.get("source")),
                _safe_text(row.get("note")),
                _safe_text(row.get("lead_detail")),
                _safe_text(row.get("date")),
            )
            if part
        )
    return any(token in blob for token in _WORKFLOW_EVENT_TOKENS)


def _is_diagnostic_factor_row(item: Mapping[str, Any]) -> bool:
    row = dict(item or {})
    blob = " ".join(
        part
        for part in (
            _safe_text(row.get("name")),
            _safe_text(row.get("signal")),
            _safe_text(row.get("detail")),
        )
        if part
    )
    return any(token in blob for token in _DIAGNOSTIC_FACTOR_TOKENS)


def _safe_lead_title(value: Any) -> str:
    title = _safe_text(value)
    if _is_placeholder_intelligence_title(title):
        return ""
    return title


def _explicit_structured_lead_detail(item: Mapping[str, Any]) -> str:
    detail = _safe_text(item.get("lead_detail"))
    if detail.startswith(("财报摘要：", "公告类型：", "政策影响层：", "主题事件：")):
        return detail
    return ""


def _layer_from_explicit_lead_detail(detail: Any) -> str:
    text = _safe_text(detail)
    if text.startswith("财报摘要："):
        return "财报"
    if text.startswith("公告类型："):
        return "公告"
    if text.startswith("政策影响层："):
        return "政策"
    if text.startswith("主题事件："):
        return "行业主题事件"
    return ""


def _candidate_from_evidence(item: Mapping[str, Any], *, as_of: Any = "", prefer_theme_event: bool = False) -> Dict[str, Any]:
    explicit_detail = _explicit_structured_lead_detail(item)
    layer = _normalize_event_layer(
        _safe_text(item.get("layer")) or _safe_text(item.get("source")),
        item.get("title"),
        item.get("signal"),
        prefer_theme_event=prefer_theme_event,
    )
    explicit_layer = _layer_from_explicit_lead_detail(explicit_detail)
    if explicit_layer:
        layer = explicit_layer
    title = _safe_text(item.get("title")) or _safe_text(item.get("signal")) or _event_title_with_source(item)
    text = _flatten_text(item, explicit_detail)
    negative = _contains_any(text, _NEGATIVE_TOKENS)
    detail = explicit_detail or _lead_detail(layer, text, negative=negative)
    axes = _impact_axes(layer, text, detail)
    event_date = _safe_text(item.get("date"))
    published_at = _safe_text(item.get("published_at"))
    as_of_dt = _parse_event_date(as_of)
    event_dt = _parse_event_date(event_date) or _parse_datetime(published_at)
    derived_age_days = abs((as_of_dt.date() - event_dt.date()).days) if as_of_dt and event_dt else None
    freshness_bucket = _safe_text(item.get("freshness_bucket"))
    age_days = item.get("age_days")
    if age_days is None:
        age_days = derived_age_days
    if not freshness_bucket and age_days is not None:
        try:
            age_value = float(age_days)
        except (TypeError, ValueError):
            age_value = None
        if age_value is not None:
            if age_value <= 3:
                freshness_bucket = "fresh"
            elif age_value <= 7:
                freshness_bucket = "recent"
            else:
                freshness_bucket = "stale"
    has_direct_source = _item_source_directness_rank(item) >= 1
    base_score = _importance_score(
        layer,
        event_date=event_date,
        as_of=as_of,
        has_direct_source=has_direct_source,
        text=text,
        detail=detail,
        axes=axes,
    )
    thesis_scope = _thesis_scope(layer, base_score, axes, detail, negative=negative)
    score = _importance_score(
        layer,
        event_date=event_date,
        as_of=as_of,
        has_direct_source=has_direct_source,
        text=text,
        detail=detail,
        axes=axes,
        thesis_scope=thesis_scope,
    )
    importance = _importance_bucket(score)
    impact_summary = _impact_summary(axes)
    signal_type = _safe_text(item.get("signal_type")) or detail or layer
    signal_strength = _signal_strength_for_scope(
        _safe_text(item.get("signal_strength")) or importance,
        thesis_scope=thesis_scope,
    )
    signal_conclusion = _signal_conclusion_text(
        lead_detail=detail,
        impact_summary=impact_summary,
        thesis_scope=thesis_scope,
        negative=negative,
        explicit=item.get("signal_conclusion"),
    )
    return {
        "layer": layer,
        "raw_layer": _safe_text(item.get("layer")),
        "title": title,
        "source": _safe_text(item.get("source")),
        "configured_source": _safe_text(item.get("configured_source")),
        "category": _safe_text(item.get("category")),
        "date": event_date,
        "link": _safe_text(item.get("link")),
        "published_at": published_at,
        "note": _safe_text(item.get("note")),
        "source_note": _safe_text(item.get("source_note")),
        "search_query": _safe_text(item.get("search_query")),
        "freshness_bucket": freshness_bucket,
        "age_days": age_days,
        "importance": importance,
        "importance_score": score,
        "negative": negative,
        "lead_detail": detail,
        "impact_axes": axes,
        "impact_summary": impact_summary,
        "thesis_scope": thesis_scope,
        "importance_reason": _importance_reason(
            "",
            layer,
            importance,
            lead_detail=detail,
            impact_summary=impact_summary,
            thesis_scope=thesis_scope,
        ),
        "signal_type": signal_type,
        "signal_strength": signal_strength,
        "signal_conclusion": signal_conclusion,
        "source_directness_rank": _item_source_directness_rank(item),
        "freshness_rank": _freshness_rank({"freshness_bucket": freshness_bucket, "age_days": age_days}),
        "source_lane_rank": _intelligence_lane_rank(
            {
                **item,
                "source_directness_rank": _item_source_directness_rank(item),
            }
        ),
        "source_tier_rank": _source_tier_rank(
            {
                **item,
                "source_directness_rank": _item_source_directness_rank(item),
            }
        ),
    }


def _event_priority_bias(item: Mapping[str, Any]) -> int:
    text = _flatten_text(
        item.get("signal_type"),
        item.get("lead_detail"),
        item.get("title"),
        item.get("source"),
        item.get("layer"),
    )
    bias = 0
    if any(token in text for token in ("标准指数框架", "指数成分权重", "标准行业归因", "行业/指数框架", "龙头权重暴露")):
        bias += 12
    if any(token in text for token in ("周线结构", "月线结构", "指数周线", "指数月线")):
        bias -= 18
    if "缺失" in text and any(token in text for token in ("周线", "月线")):
        bias -= 12
    return bias


def sort_event_items(
    items: Sequence[Mapping[str, Any]],
    *,
    as_of: Any = "",
    prefer_theme_event: bool = False,
    previous_reviewed_at: Any = None,
) -> List[Dict[str, Any]]:
    ranked: List[Dict[str, Any]] = []
    for index, item in enumerate(list(items or [])):
        row = dict(item or {})
        if not (_safe_text(row.get("title")) or _safe_text(row.get("signal"))):
            continue
        candidate = _candidate_from_evidence(row, as_of=as_of, prefer_theme_event=prefer_theme_event)
        final_scope = _thesis_scope(
            _safe_text(candidate.get("layer")),
            int(candidate.get("importance_score", 0) or 0),
            list(candidate.get("impact_axes") or []),
            _safe_text(candidate.get("lead_detail")),
            negative=bool(candidate.get("negative")),
            status="已消化",
        )
        candidate["thesis_scope"] = final_scope
        candidate["importance_reason"] = _importance_reason(
            "",
            _safe_text(candidate.get("layer")),
            _safe_text(candidate.get("importance")),
            lead_detail=_safe_text(candidate.get("lead_detail")),
            impact_summary=_safe_text(candidate.get("impact_summary")),
            thesis_scope=final_scope,
        )
        candidate["signal_type"] = _safe_text(candidate.get("signal_type")) or _safe_text(candidate.get("lead_detail")) or _safe_text(candidate.get("layer"))
        candidate["signal_strength"] = _signal_strength_for_scope(
            _safe_text(candidate.get("signal_strength")) or candidate.get("importance"),
            thesis_scope=final_scope,
        )
        candidate["signal_conclusion"] = _signal_conclusion_text(
            lead_detail=_safe_text(candidate.get("lead_detail")),
            impact_summary=_safe_text(candidate.get("impact_summary")),
            thesis_scope=final_scope,
            negative=bool(candidate.get("negative")),
            explicit=candidate.get("signal_conclusion"),
        )
        candidate["_index"] = index
        candidate["_raw"] = row
        ranked.append(candidate)
    ranked.sort(
        key=lambda item: (
            -(int(item.get("importance_score", 0)) + _event_priority_bias(item)),
            -_since_last_review_rank(item, previous_reviewed_at),
            -int(item.get("source_lane_rank", 0)),
            -int(item.get("freshness_rank", 0)),
            -int(item.get("source_tier_rank", 0)),
            -int(_parse_event_date(item.get("date")).timestamp()) if _parse_event_date(item.get("date")) else 0,
            int(item.get("_index", 0)),
        ),
    )
    return ranked


def _candidate_from_factor(item: Mapping[str, Any], *, as_of: Any = "") -> Dict[str, Any]:
    text = _flatten_text(item)
    layer = _normalize_event_layer(item.get("name"), item.get("signal"), item.get("detail"))
    title = _safe_text(item.get("signal")) or _safe_text(item.get("detail")) or _safe_text(item.get("name"))
    negative = _contains_any(text, _NEGATIVE_TOKENS)
    detail = _lead_detail(layer, text, negative=negative)
    axes = _impact_axes(layer, text, detail)
    base_score = _importance_score(
        layer,
        as_of=as_of,
        has_direct_source=True,
        text=text,
        detail=detail,
        axes=axes,
    ) - 8
    thesis_scope = _thesis_scope(layer, base_score, axes, detail, negative=negative)
    score = max(
        min(
            _importance_score(
                layer,
                as_of=as_of,
                has_direct_source=True,
                text=text,
                detail=detail,
                axes=axes,
                thesis_scope=thesis_scope,
            )
            - 8,
            100,
        ),
        0,
    )
    importance = _importance_bucket(score)
    impact_summary = _impact_summary(axes)
    age_days = _extract_factor_age_days(text)
    freshness_bucket = ""
    if age_days is not None:
        if age_days <= 3:
            freshness_bucket = "fresh"
        elif age_days <= 7:
            freshness_bucket = "recent"
        else:
            freshness_bucket = "stale"
    signal_type = detail or layer
    signal_strength = _signal_strength_for_scope(importance, thesis_scope=thesis_scope)
    signal_conclusion = _signal_conclusion_text(
        lead_detail=detail,
        impact_summary=impact_summary,
        thesis_scope=thesis_scope,
        negative=negative,
    )
    return {
        "layer": layer,
        "title": title,
        "source": _safe_text(item.get("name")),
        "date": "",
        "freshness_bucket": freshness_bucket,
        "age_days": age_days,
        "importance": importance,
        "importance_score": score,
        "negative": negative,
        "lead_detail": detail,
        "impact_axes": axes,
        "impact_summary": impact_summary,
        "thesis_scope": thesis_scope,
        "importance_reason": _importance_reason(
            "",
            layer,
            importance,
            lead_detail=detail,
            impact_summary=impact_summary,
            thesis_scope=thesis_scope,
        ),
        "signal_type": signal_type,
        "signal_strength": signal_strength,
        "signal_conclusion": signal_conclusion,
    }


def _candidate_from_text(text: Any, *, as_of: Any = "") -> Dict[str, Any]:
    content = _safe_text(text)
    layer = _normalize_event_layer("", content, "", prefer_theme_event=True)
    negative = _contains_any(content, _NEGATIVE_TOKENS)
    detail = _lead_detail(layer, content, negative=negative)
    axes = _impact_axes(layer, content, detail)
    base_score = _importance_score(layer, as_of=as_of, text=content, detail=detail, axes=axes)
    thesis_scope = _thesis_scope(layer, base_score, axes, detail, negative=negative)
    score = _importance_score(layer, as_of=as_of, text=content, detail=detail, axes=axes, thesis_scope=thesis_scope)
    importance = _importance_bucket(score)
    impact_summary = _impact_summary(axes)
    signal_type = detail or layer
    signal_strength = _signal_strength_for_scope(importance, thesis_scope=thesis_scope)
    signal_conclusion = _signal_conclusion_text(
        lead_detail=detail,
        impact_summary=impact_summary,
        thesis_scope=thesis_scope,
        negative=negative,
    )
    return {
        "layer": layer,
        "title": content,
        "source": "briefing",
        "date": "",
        "importance": importance,
        "importance_score": score,
        "negative": negative,
        "lead_detail": detail,
        "impact_axes": axes,
        "impact_summary": impact_summary,
        "thesis_scope": thesis_scope,
        "importance_reason": _importance_reason(
            "",
            layer,
            importance,
            lead_detail=detail,
            impact_summary=impact_summary,
            thesis_scope=thesis_scope,
        ),
        "signal_type": signal_type,
        "signal_strength": signal_strength,
        "signal_conclusion": signal_conclusion,
    }


def _candidate_from_market_event_row(row: Any, *, as_of: Any = "") -> Dict[str, Any]:
    values = list(row or [])
    title = _safe_text(values[1] if len(values) > 1 else "")
    if not title:
        return {}
    source = _safe_text(values[2] if len(values) > 2 else "") or "市场情报"
    date = _safe_text(values[0] if len(values) > 0 else "")
    if _is_workflow_event_row({"title": title, "source": source, "date": date}):
        return {}
    signal_type = _safe_text(values[6] if len(values) > 6 else "") or "主题/市场情报"
    conclusion = _safe_text(values[7] if len(values) > 7 else "")
    link = _safe_text(values[5] if len(values) > 5 else "")
    impact = _safe_text(values[4] if len(values) > 4 else "")
    strength = _safe_text(values[3] if len(values) > 3 else "")
    item = {
        "title": title,
        "source": source,
        "date": date,
        "link": link,
        "lead_detail": f"主题事件：{signal_type}" + (f"；结论：{conclusion}" if conclusion else ""),
        "layer": "行业主题事件",
        "importance": {"高": "high", "中": "medium", "低": "low"}.get(strength, "medium"),
        "impact_summary": impact,
        "signal_type": signal_type,
        "signal_strength": strength,
        "signal_conclusion": conclusion,
    }
    return _candidate_from_evidence(item, as_of=as_of, prefer_theme_event=True)


def _collect_candidates(
    payload: Mapping[str, Any],
    *,
    as_of: Any = "",
    previous_reviewed_at: Any = None,
) -> List[Dict[str, Any]]:
    candidates: List[Dict[str, Any]] = []
    dimensions = dict(payload.get("dimensions") or {})
    catalyst = dict(dimensions.get("catalyst") or {})
    for item in list(catalyst.get("evidence") or []):
        row = dict(item or {})
        if _is_placeholder_intelligence_title(row.get("title")) and not _explicit_structured_lead_detail(row):
            continue
        if _is_diagnostic_intelligence_row(row):
            continue
        candidates.append(_candidate_from_evidence(row, as_of=as_of))
    for item in list(catalyst.get("theme_news") or []):
        row = dict(item or {})
        if _is_placeholder_intelligence_title(row.get("title")) and not _explicit_structured_lead_detail(row):
            continue
        candidates.append(_candidate_from_evidence(row, as_of=as_of, prefer_theme_event=True))
    for item in list(dict(payload.get("news_report") or {}).get("items") or []):
        row = dict(item or {})
        if _is_placeholder_intelligence_title(row.get("title")):
            continue
        if _is_diagnostic_intelligence_row(row):
            continue
        if _is_workflow_event_row(row):
            continue
        candidates.append(_candidate_from_evidence(row, as_of=as_of))
    for row in list(payload.get("market_event_rows") or []):
        candidate = _candidate_from_market_event_row(row, as_of=as_of)
        if candidate:
            candidates.append(candidate)
    if not candidates:
        for item in list(catalyst.get("factors") or []):
            row = dict(item or {})
            if _is_placeholder_intelligence_title(row.get("signal")):
                continue
            if _is_diagnostic_factor_row(row):
                continue
            candidates.append(_candidate_from_factor(row, as_of=as_of))
    if not candidates:
        for item in [*(payload.get("core_event_lines") or []), *(payload.get("headline_lines") or [])]:
            if _is_placeholder_intelligence_title(item):
                continue
            if _is_workflow_event_row(item):
                continue
            candidates.append(_candidate_from_text(item, as_of=as_of))
    unique: List[Dict[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for item in candidates:
        key = (_safe_text(item.get("layer")), _safe_text(item.get("title")))
        if key in seen:
            continue
        seen.add(key)
        unique.append(item)
    def _sort_key(item: Mapping[str, Any]) -> tuple[int, int, int, float, str]:
        event_dt = _parse_event_date(item.get("date") or item.get("published_at") or item.get("as_of"))
        event_rank = -(event_dt.timestamp()) if event_dt else 0.0
        return (
            -(int(item.get("importance_score", 0)) + _event_priority_bias(item)),
            -_since_last_review_rank(item, previous_reviewed_at),
            -int(item.get("source_lane_rank", 0) or 0),
            -int(item.get("freshness_rank", 0) or 0),
            -int(item.get("source_tier_rank", 0) or 0),
            event_rank,
            _safe_text(item.get("title")),
        )

    unique.sort(key=_sort_key)
    return unique[:6]


def _latest_signal_at(payload: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]]) -> str:
    catalyst = dict(dict(payload.get("dimensions") or {}).get("catalyst") or {})
    coverage = dict(catalyst.get("coverage") or {})
    latest_coverage = _safe_text(coverage.get("latest_news_at"))
    if latest_coverage:
        return latest_coverage
    latest_stamp: datetime | None = None
    latest_text = ""
    for item in candidates:
        for key in ("date", "published_at", "as_of"):
            value = _safe_text(item.get(key))
            stamp = _parse_datetime(value)
            if stamp is None:
                continue
            if latest_stamp is None or stamp > latest_stamp:
                latest_stamp = stamp
                latest_text = value
    return latest_text


def _history_note(payload: Mapping[str, Any], latest_signal_at: str, previous_reviewed_at: str) -> str:
    previous_text = _safe_text(previous_reviewed_at)
    if not previous_text:
        return ""
    previous_stamp = _parse_datetime(previous_text)
    latest_stamp = _parse_datetime(latest_signal_at)
    catalyst = dict(dict(payload.get("dimensions") or {}).get("catalyst") or {})
    coverage = dict(catalyst.get("coverage") or {})
    diagnosis = _safe_text(coverage.get("diagnosis"))
    if diagnosis == "stale_live_only":
        return f"自上次复查（`{previous_text}`）以来暂无新增高置信情报，当前更多是旧闻回放或背景线索。"
    if diagnosis == "theme_only_live":
        return f"自上次复查（`{previous_text}`）以来有主题级新增情报，但公司/产品级直连催化仍不足。"
    if latest_stamp is None:
        return f"自上次复查（`{previous_text}`）以来，当前还没有可确认时间戳的新增高置信情报。"
    if previous_stamp is not None and latest_stamp <= previous_stamp:
        return f"自上次复查（`{previous_text}`）以来暂无更晚的高置信情报，当前不把旧线索直接当成新增催化。"
    return f"自上次复查（`{previous_text}`）以来，已出现更新的情报线索，最新时间 `{latest_signal_at}`。"


def review_history_context_text(payload: Mapping[str, Any]) -> str:
    digest = dict(payload or {})
    history_note = _safe_text(digest.get("history_note"))
    if history_note:
        return history_note
    previous_reviewed_at = _safe_text(digest.get("previous_reviewed_at"))
    if previous_reviewed_at:
        return f"自上次复查（`{previous_reviewed_at}`）以来，当前先把新旧线索分开看。"
    if digest:
        return "这是首次跟踪，当前先建立情报基线。"
    return ""


def _pending_review(payload: Mapping[str, Any]) -> bool:
    dimensions = dict(payload.get("dimensions") or {})
    catalyst = dict(dimensions.get("catalyst") or {})
    web_review = dict(payload.get("catalyst_web_review") or catalyst.get("web_review") or {})
    if web_review and not bool(web_review.get("completed")):
        return True
    diagnosis = _flatten_text(
        dict(payload.get("provenance") or {}).get("catalyst_diagnosis"),
        catalyst.get("diagnosis"),
        payload.get("coverage_note"),
        payload.get("quality_lines"),
        payload.get("headline_lines"),
        payload.get("core_event_lines"),
    )
    return any(token in diagnosis for token in ("suspected_search_gap", "待 AI 联网复核", "待复核"))


def _fallback_layer(theme_playbook: Mapping[str, Any] | None, payload: Mapping[str, Any]) -> str:
    playbook = dict(theme_playbook or {})
    if _safe_text(playbook.get("label")) or _safe_text(payload.get("day_theme")):
        return "行业主题事件"
    return "新闻"


def _status(payload: Mapping[str, Any], candidates: Sequence[Mapping[str, Any]], theme_playbook: Mapping[str, Any] | None = None) -> str:
    if _pending_review(payload):
        return "待复核"
    if candidates:
        return "已消化"
    if _fallback_layer(theme_playbook, payload):
        return "待补充"
    return "待补充"


def _changed_what_text(
    status: str,
    layer: str,
    *,
    negative: bool = False,
    lead_detail: str = "",
    impact_summary: str = "",
    thesis_scope: str = "",
    stale_baseline: bool = False,
) -> str:
    impact_part = f"更直接改的是 `{impact_summary or '资金偏好'}` 这层。" if impact_summary else ""
    if stale_baseline and status != "待复核":
        return (
            f"`{lead_detail or layer or '事件'}` 当前更多是旧闻回放或历史基线，{impact_part}"
            " 它更适合帮助理解这条线原本怎么演变，不直接当成本轮新增催化。"
        ).strip()
    if status == "待复核":
        return (
            f"当前首先改变的是证据边界：`{lead_detail or layer or '事件'}` 这条线可能在改写研究重心，"
            f"{impact_part} 但它现在还只能按 `{thesis_scope or '待确认'}` 处理，先补联网复核。"
        ).strip()
    if status == "待补充":
        if layer == "政策":
            return (
                f"当前只够把它当 `{lead_detail or '政策线索'}` 处理，{impact_part}"
                " 下一步要补执行细则、受益链条和时间线，不能直接写成已经兑现。"
            ).strip()
        if layer == "行业主题事件":
            return (
                f"当前更多只是把研究焦点抬到 `{lead_detail or '主题/产业链'}` 这层，{impact_part}"
                " 还缺公司或产品级直连事件，不能把背景热度直接写成可执行催化。"
            ).strip()
        return (
            f"当前更像 `{lead_detail or layer or '事件'}` 的补证据阶段，{impact_part}"
            " 下一步要补更直接的公司/产品级事件，不把信息空白误读成高确定性判断。"
        ).strip()
    if thesis_scope == "一次性噪音":
        return (
            f"`{lead_detail or layer or '事件'}` 这条线目前更多像一次性噪音，{impact_part}"
            " 还不足以单独改写 thesis，更适合当辅助理解而不是直接升级动作。"
        ).strip()
    if layer == "财报":
        if "毛利率/费用率改善" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到利润率改善和经营杠杆层，{impact_part}"
                " 后面更该看价格、成本和费用纪律能不能继续兑现，而不是只看一季报表观改善。"
            ).strip()
        if "现金流/合同负债改善" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到回款质量、订单前瞻和兑现质量层，{impact_part}"
                " 但当前更该先看回款持续性、合同负债留存和收入落地，不把财务质量改善直接写成利润已经稳定抬升。"
            ).strip()
        if "存货/减值压力" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到库存去化、资产质量和需求承压层，{impact_part}"
                " 后面更该看减值范围、去库节奏和终端修复，而不是把单次计提直接写成影响已经出清。"
            ).strip()
        if "资本开支/扩产" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到产能投入和未来兑现层，{impact_part}"
                " 但当前更该先验证扩产节奏、需求承接和投入回报，而不是直接把它当成当期盈利已经兑现。"
            ).strip()
        if "回购/分红" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到股东回报和估值支撑层，{impact_part}"
                " 但当前更该先看回购执行、分红持续性和资金偏好会不会真的改善。"
            ).strip()
        return (
            f"`{lead_detail or '财报'}` 已把研究重点推进到财务兑现层，{impact_part}"
            + (
                " 后面更该看盈利下修或兑现压力会不会继续扩散。"
                if negative
                else " 后面更该看财报质量、指引和资本开支能不能持续支撑估值。"
            )
        ).strip()
    if layer == "公告":
        if "投资者关系/路演纪要" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到管理层表述、经营口径和景气预期层，{impact_part}"
                " 但当前更该先看这些表述能否被后续订单、财报或价格确认兑现，而不是把纪要口径直接写成结论。"
            ).strip()
        if "问询/回复函" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到监管关切、会计口径和信息澄清层，{impact_part}"
                " 但当前更该先看回复是否真正消除不确定性，以及后续有没有财务和经营层面的兑现。"
            ).strip()
        if "产品/新品" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到产品验证、客户反馈和需求映射层，{impact_part}"
                " 后面更该看新品放量、客户接受度和产业链映射会不会真的传到盈利。"
            ).strip()
        if "融资/定增" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到资本结构、募资投向和潜在摊薄层，{impact_part}"
                " 但当前更该先看发行条款、募资用途和锁定安排，不把融资标题直接写成基本面已经改善。"
            ).strip()
        if "并购/重组" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到外延整合、资产注入和估值重定价层，{impact_part}"
                " 但当前更该先看审批进度、交易对价和整合兑现，不把交易标题直接写成协同已经落地。"
            ).strip()
        if "减持/解禁" in lead_detail:
            return (
                f"`{lead_detail}` 已把研究重点推进到筹码供给、资金承接和风险偏好层，{impact_part}"
                " 但当前更该先看减持规模、解禁节奏和承接能力，不把筹码事件直接写成趋势已经彻底破坏。"
            ).strip()
        return (
            f"`{lead_detail or '公告'}` 已把研究重点推进到公司/产品级执行层，{impact_part}"
            + (
                " 后面更该看风险影响范围、修复条件和时间窗。"
                if negative
                else " 后面更该看订单、回购、项目节奏或资本动作会不会真的兑现。"
            )
        ).strip()
    if layer == "政策":
        if "财政支持/名单落地" in lead_detail:
            return (
                f"`{lead_detail}` 已把焦点从政策标题推进到资金下达、名单目录和受益链条层，{impact_part}"
                " 当前先回答哪些环节真正拿到支持、预算强度够不够、兑现时点在哪里。"
            ).strip()
        if "价格机制/收费调整" in lead_detail:
            return (
                f"`{lead_detail}` 已把焦点推进到价格传导、收益口径和估值重定价层，{impact_part}"
                " 但当前更该先看执行口径、传导滞后和覆盖范围，不把机制标题直接写成利润已经兑现。"
            ).strip()
        if "配套细则" in lead_detail:
            return (
                f"`{lead_detail}` 已把焦点从政策标题推进到执行框架层，{impact_part}"
                " 但当前更该先验证配套口径、覆盖范围和兑现节奏，不把细则出台直接写成产业链已经兑现。"
            ).strip()
        if "方向表态" in lead_detail:
            return (
                f"`{lead_detail}` 当前更多是在抬升政策预期和风险偏好，{impact_part}"
                " 还要继续补受益链条、执行主体和时间线，不能直接写成 thesis 已经被坐实。"
            ).strip()
        return (
            f"`{lead_detail or '政策'}` 已把焦点从单一标题，拉到受益链条、执行细则和落地时间线这层；{impact_part}"
            " 当前先回答谁真正受益、什么时候兑现。"
        ).strip()
    if layer == "行业主题事件":
        if "价格/排产验证" in lead_detail:
            return (
                f"`{lead_detail}` 已把判断从泛主题热度推进到供需、价格和排产验证层，{impact_part}"
                " 后面更该看涨价传导、排产持续性和受益顺序，而不是只看单日情绪。"
            ).strip()
        if "大会/产品催化" in lead_detail:
            return (
                f"`{lead_detail}` 已把焦点推进到新品节奏、主题扩散和预期抬升层，{impact_part}"
                " 但当前更该先看客户反馈、后续订单和产业链映射，不把大会热度直接写成盈利兑现。"
            ).strip()
        if "海外映射/链式催化" in lead_detail:
            return (
                f"`{lead_detail}` 已把焦点推进到海外事件外溢和本土映射层，{impact_part}"
                " 但当前更该先找国内链条的公司级直连证据，不把海外强势直接平移成本土 thesis。"
            ).strip()
        if "情绪热度" in lead_detail:
            return (
                f"`{lead_detail}` 当前更多是在抬升市场关注度和资金偏好层，{impact_part}"
                " 还缺景气、价格或公司级兑现，不能把情绪热度直接写成产业链已经验证。"
            ).strip()
        return (
            f"`{lead_detail or '主题事件'}` 已把判断从单票噪音抬到主题/产业链层，{impact_part}"
            " 后面更该看扩散路径、受益顺序和主线持续性，而不是只看一条标题。"
        ).strip()
    return (
        f"更多改变的是信息环境和市场关注度，{impact_part}"
        " 说明这条线开始被更多人看到，但还不能自动等同为直接催化已经兑现。"
    ).strip()


def _next_step_text(status: str, layer: str, *, lead_detail: str = "", thesis_scope: str = "") -> str:
    if thesis_scope == "历史基线":
        return "继续找新增公司/产品级直连情报和价格确认，别把旧闻直接升级成新动作。"
    if status == "待复核":
        return f"先补 `{lead_detail or layer or '事件'}` 的独立联网复核，再决定是否把它升级成直接催化。"
    if status == "待补充":
        if layer == "政策":
            return "补政策细则、执行阶段和受益链条。"
        if layer == "行业主题事件":
            return "补公司/产品级直连事件，确认这条主题有没有真正落到标的。"
        return "补更直接的公司/产品级事件或执行细节。"
    if thesis_scope == "一次性噪音":
        return "继续盯价格确认和后续证据，先别把单条标题直接升级成 thesis。"
    if layer == "财报":
        if "毛利率/费用率改善" in lead_detail:
            return "继续盯毛利率、费用纪律、价格传导和利润率持续性。"
        if "现金流/合同负债改善" in lead_detail:
            return "继续盯回款持续性、合同负债留存、收入兑现和现金流质量。"
        if "存货/减值压力" in lead_detail:
            return "继续盯去库节奏、减值范围、终端需求和修复条件。"
        if "资本开支/扩产" in lead_detail:
            return "继续盯资本开支投向、扩产节奏、需求承接和投入回报。"
        if "回购/分红" in lead_detail:
            return "继续盯回购执行、分红持续性和资金偏好反馈。"
        return "继续盯下一季指引、利润兑现质量和价格确认。"
    if layer == "公告":
        if "投资者关系/路演纪要" in lead_detail:
            return "继续盯纪要口径能否被订单、财报、价格和后续公告验证。"
        if "问询/回复函" in lead_detail:
            return "继续盯监管问询是否收口、回复是否消除不确定性，以及后续财务兑现。"
        if "产品/新品" in lead_detail:
            return "继续盯新品验证、客户反馈、放量节奏和价格确认。"
        if "融资/定增" in lead_detail:
            return "继续盯发行条款、募资用途、摊薄影响和锁定安排。"
        if "并购/重组" in lead_detail:
            return "继续盯审批进度、交易对价、整合节奏和协同兑现。"
        if "减持/解禁" in lead_detail:
            return "继续盯减持规模、解禁节奏、承接能力和价格反馈。"
        return "继续盯公告落地节奏、兑现质量和价格反馈。"
    if layer == "政策":
        if "财政支持/名单落地" in lead_detail:
            return "继续盯预算强度、名单覆盖、执行主体和项目兑现时点。"
        if "价格机制/收费调整" in lead_detail:
            return "继续盯执行口径、价格传导、覆盖范围和兑现滞后。"
        if "配套细则" in lead_detail:
            return "继续盯配套口径、覆盖范围、执行主体和兑现节奏。"
        if "方向表态" in lead_detail:
            return "继续盯后续细则、执行主体和受益链条，不把表态直接当落地。"
        return "继续盯细则、配套资金和项目落地时间点。"
    if layer == "行业主题事件":
        if "价格/排产验证" in lead_detail:
            return "继续盯涨价传导、排产持续性、供需强弱和受益顺序。"
        if "大会/产品催化" in lead_detail:
            return "继续盯客户反馈、后续订单、产品验证和主题扩散。"
        if "海外映射/链式催化" in lead_detail:
            return "继续盯国内链条直连证据、映射强度和本土价格反馈。"
        if "情绪热度" in lead_detail:
            return "继续盯热度是否转成景气、价格或公司级直连事件。"
        return "继续盯主线是否扩散到产业链和代表标的。"
    return "继续盯关注度能否转成更直接的催化或价格确认。"


def _importance_reason(
    status: str,
    layer: str,
    importance: str,
    *,
    lead_detail: str = "",
    impact_summary: str = "",
    thesis_scope: str = "",
) -> str:
    impact_part = impact_summary or "资金偏好"
    if thesis_scope == "历史基线" and status != "待复核":
        return (
            f"先把它当历史基线看，因为 `{lead_detail or layer or '事件'}` 属于旧闻回放；"
            f"它能帮助理解 `{impact_part}` 的来龙去脉，但先不当成新增驱动。"
        )
    if status == "待复核":
        return f"必须前置复核，因为 `{lead_detail or layer or '事件'}` 可能改写 `{impact_part}`，但证据边界还没收口。"
    if thesis_scope == "一次性噪音":
        return f"先不升级优先级，因为 `{lead_detail or layer or '事件'}` 更像一次性噪音，只把它当 `{impact_part}` 的辅助线索。"
    if layer == "财报":
        if "毛利率/费用率改善" in lead_detail:
            return f"优先前置，因为利润率和费用率改善已经开始改写 `{impact_part}`。"
        if "现金流/合同负债改善" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为它更偏 `{impact_part}` 的前瞻验证，还要看回款持续性和收入兑现。"
        if "存货/减值压力" in lead_detail:
            return f"优先前置，因为库存和减值压力已经开始改写 `{impact_part}`。"
        if "资本开支/扩产" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为它更偏 `{impact_part}` 的未来兑现，还要等扩产节奏和需求承接验证。"
        if "回购/分红" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为它更偏 `{impact_part}`，还要看回购执行和分红持续性。"
        if thesis_scope == "thesis变化":
            return f"优先前置，因为公司级财报已经直接改写 `{impact_part}`。"
    if layer == "公告":
        if "投资者关系/路演纪要" in lead_detail:
            return f"保留前排观察，因为管理层口径已开始影响 `{impact_part}`，但还要看订单、财报和价格确认是否跟上。"
        if "问询/回复函" in lead_detail:
            return f"保留前排观察，因为问询/回复会影响 `{impact_part}` 的理解，但还要看回复质量和后续兑现能否收口。"
        if "产品/新品" in lead_detail:
            return f"保留前排观察，因为新品已开始影响 `{impact_part}`，但还要看产品验证和放量兑现。"
        if "融资/定增" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为它更偏 `{impact_part}` 的资本结构变化，还要看发行条款、募资用途和摊薄影响。"
        if "并购/重组" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为它会影响 `{impact_part}`，还要看审批、交易对价和整合兑现。"
        if "减持/解禁" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为它更偏 `{impact_part}` 的筹码压力，还要看减持规模、解禁节奏和承接能力。"
        if thesis_scope == "thesis变化":
            return f"优先前置，因为公司级执行事件已开始改写 `{impact_part}`。"
    if layer == "政策":
        if "财政支持/名单落地" in lead_detail:
            return f"优先前置，因为政策资金和名单已经落到执行层，开始改写 `{impact_part}`。"
        if "价格机制/收费调整" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为价格机制会影响 `{impact_part}`，还要看执行口径、传导滞后和覆盖范围。"
        if "配套细则" in lead_detail:
            return f"放到前排跟踪，但先不直接升级，因为政策只推进到执行框架层，还要看覆盖范围和兑现节奏。"
        if "方向表态" in lead_detail:
            return f"保留观察优先级，因为政策更多在抬升 `{impact_part}`，仍缺执行主体和受益链条。"
        if thesis_scope == "thesis变化":
            return f"优先前置，因为政策已进入直接执行层，开始改写 `{impact_part}`。"
    if layer == "行业主题事件":
        if "价格/排产验证" in lead_detail:
            return f"优先前置，因为主题景气和价格验证已开始改写 `{impact_part}`。"
        if "大会/产品催化" in lead_detail:
            return f"保留前排观察，因为大会和产品催化已开始影响 `{impact_part}`，但还要看订单和验证兑现。"
        if "海外映射/链式催化" in lead_detail:
            return f"保留观察优先级，因为海外映射会影响 `{impact_part}`，但还缺国内链条直连证据。"
        if "情绪热度" in lead_detail:
            return f"先放在观察前排，因为它更多是在抬升 `{impact_part}`，还没下沉成景气或公司级兑现。"
        return f"先放在观察前排，因为它更多是在改写 `{impact_part}` 的主题理解，还没下沉成公司级兑现。"
    if importance == "high":
        return f"优先前置，因为这条线已经开始改写 `{impact_part}`。"
    if importance == "medium":
        return f"保留中等优先级，因为它会影响 `{impact_part}`，但还需要后续验证。"
    return f"先放在后排，因为它暂时更像信息环境变化，还不足以单独改写 `{impact_part}`。"


def compact_importance_reason(value: Any) -> str:
    text = _safe_text(value).rstrip("。")
    if not text:
        return ""
    replacements = (
        ("必须前置复核，因为", "前置理由："),
        ("优先前置，因为", "前置理由："),
        ("保留前排观察，因为", "先观察，因为"),
        ("放到前排跟踪，但先不直接升级，因为", "先观察，因为"),
        ("保留观察优先级，因为", "先观察，因为"),
        ("先放在观察前排，因为", "先观察，因为"),
        ("先不升级优先级，因为", "先不升级，因为"),
        ("保留中等优先级，因为", "先观察，因为"),
        ("先放在后排，因为", "后排原因："),
    )
    for prefix, replacement in replacements:
        if text.startswith(prefix):
            return f"{replacement}{text[len(prefix):].strip()}"
    return text


def build_event_digest(
    payload: Mapping[str, Any],
    *,
    theme_playbook: Mapping[str, Any] | None = None,
    previous_reviewed_at: str = "",
) -> Dict[str, Any]:
    source = dict(payload or {})
    symbol = _safe_text(source.get("symbol"))
    as_of = (
        _safe_text(source.get("generated_at"))
        or _safe_text(dict(source.get("provenance") or {}).get("analysis_generated_at"))
        or _safe_text(dict(source.get("provenance") or {}).get("catalyst_evidence_as_of"))
    )
    candidates = _collect_candidates(source, as_of=as_of, previous_reviewed_at=previous_reviewed_at)
    status = _status(source, candidates, theme_playbook)
    lead = dict(candidates[0]) if candidates else {}
    lead_layer = _safe_text(lead.get("layer")) or _fallback_layer(theme_playbook, source)
    negative = bool(lead.get("negative"))
    lead_detail = _safe_text(lead.get("lead_detail")) or _lead_detail(lead_layer, _safe_text(lead.get("title")), negative=negative)
    impact_axes = list(lead.get("impact_axes") or _impact_axes(lead_layer, _safe_text(lead.get("title")), lead_detail))
    impact_summary = _safe_text(lead.get("impact_summary")) or _impact_summary(impact_axes)
    provisional_scope = _thesis_scope(
        lead_layer,
        int(lead.get("importance_score", 0) or 0),
        impact_axes,
        lead_detail,
        negative=negative,
        status=status,
    )
    lead_tags = intelligence_attribute_labels(
        {
            **lead,
            "layer": _safe_text(lead.get("raw_layer")) or _safe_text(lead.get("layer")) or lead_layer,
            "lead_detail": lead_detail,
        },
        as_of=as_of,
        first_tracking=not _safe_text(previous_reviewed_at),
        previous_reviewed_at=previous_reviewed_at,
    )
    stale_baseline = "旧闻回放" in lead_tags and "新鲜情报" not in lead_tags
    thesis_scope = "历史基线" if stale_baseline and status != "待复核" else provisional_scope
    changed_what = _changed_what_text(
        status,
        lead_layer,
        negative=negative,
        lead_detail=lead_detail,
        impact_summary=impact_summary,
        thesis_scope=thesis_scope,
        stale_baseline=stale_baseline,
    )
    next_step = _next_step_text(status, lead_layer, lead_detail=lead_detail, thesis_scope=thesis_scope)
    importance = "high" if status == "待复核" else (_safe_text(lead.get("importance")) or ("low" if status == "待补充" else "medium"))
    importance_reason = _importance_reason(
        status,
        lead_layer,
        importance,
        lead_detail=lead_detail,
        impact_summary=impact_summary,
        thesis_scope=thesis_scope,
    )
    label = _safe_text(dict(theme_playbook or {}).get("label")) or _safe_text(source.get("name")) or _safe_text(source.get("symbol"))
    latest_signal_at = _latest_signal_at(source, candidates)
    history_note = _history_note(source, latest_signal_at, previous_reviewed_at)
    intelligence_attributes = lead_tags or intelligence_attribute_labels(
        {
            **lead,
            "layer": _safe_text(lead.get("raw_layer")) or _safe_text(lead.get("layer")) or lead_layer,
            "lead_detail": lead_detail,
        },
        as_of=as_of or latest_signal_at or previous_reviewed_at,
        first_tracking=not _safe_text(previous_reviewed_at),
        previous_reviewed_at=previous_reviewed_at,
    )
    asset_label = _safe_text(source.get("name")) or _safe_text(source.get("symbol"))
    title = _safe_lead_title(lead.get("title"))
    lead_link = effective_intelligence_link(lead, symbol=symbol) if (title or lead_detail) else ""
    signal_type = _safe_text(lead.get("signal_type")) or lead_detail or lead_layer
    signal_strength = _signal_strength_for_scope(
        _safe_text(lead.get("signal_strength")) or importance,
        thesis_scope=thesis_scope,
        status=status,
    )
    signal_conclusion = _signal_conclusion_text(
        status=status,
        lead_detail=lead_detail,
        impact_summary=impact_summary,
        thesis_scope=thesis_scope,
        negative=negative,
        explicit=lead.get("signal_conclusion"),
    )
    if not title and lead_link and lead_detail:
        title = f"{asset_label or '标的'} 官方披露查询"
    return {
        "contract_version": "event_digest.v1",
        "symbol": symbol,
        "as_of": as_of,
        "status": status,
        "lead_layer": lead_layer,
        "lead_detail": lead_detail,
        "importance": importance,
        "importance_label": _IMPORTANCE_LABELS.get(importance, importance or "中"),
        "lead_title": title,
        "lead_link": lead_link if title else "",
        "impact_axes": impact_axes,
        "impact_summary": impact_summary,
        "thesis_scope": thesis_scope,
        "importance_reason": importance_reason,
        "changed_what": changed_what,
        "next_step": next_step,
        "theme_label": label,
        "latest_signal_at": latest_signal_at,
        "previous_reviewed_at": _safe_text(previous_reviewed_at),
        "history_note": history_note,
        "intelligence_attributes": intelligence_attributes,
        "signal_type": signal_type,
        "signal_strength": signal_strength,
        "signal_conclusion": signal_conclusion,
        "items": candidates,
    }


def summarize_event_digest_contract(payload: Mapping[str, Any]) -> Dict[str, Any]:
    digest = dict(payload or {})
    if not digest:
        return {}
    summary = {
        "contract_version": "event_digest.v1",
        "status": _safe_text(digest.get("status")),
        "lead_layer": _safe_text(digest.get("lead_layer")),
        "lead_detail": _safe_text(digest.get("lead_detail")),
        "importance": _safe_text(digest.get("importance")),
        "lead_title": _safe_text(digest.get("lead_title")),
        "lead_link": _safe_text(digest.get("lead_link")),
        "impact_summary": _safe_text(digest.get("impact_summary")),
        "thesis_scope": _safe_text(digest.get("thesis_scope")),
        "importance_reason": _safe_text(digest.get("importance_reason")),
        "changed_what": _safe_text(digest.get("changed_what")),
        "next_step": _safe_text(digest.get("next_step")),
        "intelligence_attributes": list(digest.get("intelligence_attributes") or []),
        "signal_type": _safe_text(digest.get("signal_type")),
        "signal_strength": _safe_text(digest.get("signal_strength")),
        "signal_conclusion": _safe_text(digest.get("signal_conclusion")),
    }
    compact: Dict[str, Any] = {}
    for key, value in summary.items():
        if isinstance(value, list):
            if value:
                compact[key] = value
            continue
        if _safe_text(value):
            compact[key] = value
    return compact


def _event_item_identity(title: Any, link: Any, layer: Any, date: Any) -> tuple[str, str, str]:
    return (
        _safe_text(title).lower(),
        _safe_text(link).lower(),
        _safe_text(layer).lower(),
    )


def _related_event_items(payload: Mapping[str, Any], *, limit: int = 3) -> List[Dict[str, Any]]:
    digest = dict(payload or {})
    symbol = _safe_text(digest.get("symbol"))
    as_of = _safe_text(digest.get("as_of")) or _safe_text(digest.get("latest_signal_at"))
    previous_reviewed_at = _safe_text(digest.get("previous_reviewed_at"))
    lead_identity = _event_item_identity(
        digest.get("lead_title"),
        digest.get("lead_link"),
        digest.get("lead_layer"),
        digest.get("latest_signal_at"),
    )
    seen: set[tuple[str, str, str]] = {lead_identity}
    related: List[Dict[str, Any]] = []
    for ranked in sort_event_items(
        list(digest.get("items") or []),
        as_of=as_of,
        previous_reviewed_at=previous_reviewed_at,
    ):
        raw = dict(ranked.get("_raw") or {})
        title = _safe_lead_title(raw.get("title") or ranked.get("title"))
        if not title:
            continue
        layer = _safe_text(ranked.get("layer")) or _safe_text(raw.get("layer")) or _safe_text(raw.get("raw_layer"))
        date = _safe_text(raw.get("date")) or _safe_text(raw.get("published_at"))
        link = effective_intelligence_link(raw, symbol=symbol)
        identity = _event_item_identity(title, link, layer, date)
        if identity in seen:
            continue
        seen.add(identity)
        lead_detail = _safe_text(ranked.get("lead_detail"))
        tags = intelligence_attribute_labels(
            {**raw, "layer": layer, "lead_detail": lead_detail},
            as_of=as_of,
            first_tracking=not previous_reviewed_at,
            previous_reviewed_at=previous_reviewed_at,
        )
        stale_baseline = "旧闻回放" in tags and "新鲜情报" not in tags
        thesis_scope = "历史基线" if stale_baseline else _safe_text(ranked.get("thesis_scope"))
        importance_reason = (
            "先把它当历史基线看，不把旧闻直接升级成新增催化"
            if stale_baseline
            else compact_importance_reason(ranked.get("importance_reason"))
        )
        related.append(
            {
                "title": title,
                "link": link,
                "layer": layer,
                "lead_detail": lead_detail,
                "importance_label": _IMPORTANCE_LABELS.get(_safe_text(ranked.get("importance")), _safe_text(ranked.get("importance")) or "中"),
                "impact_summary": _safe_text(ranked.get("impact_summary")),
                "thesis_scope": thesis_scope,
                "importance_reason": importance_reason,
                "source": _safe_text(raw.get("source")),
                "date": date,
                "intelligence_attributes": tags,
                "source_lane": intelligence_source_lane(tags),
                "signal_type": _safe_text(ranked.get("signal_type")) or lead_detail or layer,
                "signal_strength": _signal_strength_for_scope(
                    _safe_text(ranked.get("signal_strength")) or ranked.get("importance"),
                    thesis_scope=thesis_scope,
                ),
                "signal_conclusion": _signal_conclusion_text(
                    lead_detail=lead_detail,
                    impact_summary=_safe_text(ranked.get("impact_summary")),
                    thesis_scope=thesis_scope,
                    negative=bool(ranked.get("negative")),
                    explicit=ranked.get("signal_conclusion"),
                ),
            }
        )
        if len(related) >= limit:
            break
    return related


def _related_item_homepage_line(item: Mapping[str, Any]) -> str:
    row = dict(item or {})
    title = _safe_text(row.get("title"))
    if not title:
        return ""
    detail = _safe_text(row.get("lead_detail")) or _safe_text(row.get("layer")) or "相关情报"
    title_text = _markdown_link(title, row.get("link"))
    suffix_parts = []
    source = _safe_text(row.get("source"))
    date = _safe_text(row.get("date"))
    if source or date:
        suffix_parts.append(" / ".join(part for part in (source, date) if part))
    suffix_parts.append(f"信号：`{_safe_text(row.get('signal_type')) or detail}`")
    suffix_parts.append(f"信号强弱：`{_safe_text(row.get('signal_strength')) or _safe_text(row.get('importance_label')) or '中'}`")
    conclusion = _safe_text(row.get("signal_conclusion"))
    if conclusion:
        suffix_parts.append(f"结论：{conclusion}")
    tags = list(row.get("intelligence_attributes") or [])
    if tags:
        suffix_parts.append(f"情报属性：{format_intelligence_attributes(tags)}")
    source_lane = _safe_text(row.get("source_lane"))
    if source_lane:
        suffix_parts.append(f"来源层级：`{source_lane}`")
    return f"相关情报补充：`{detail}` {_markdown_link(title, row.get('link'))}（{'；'.join(suffix_parts)}）"


def _related_item_section_lines(item: Mapping[str, Any]) -> List[str]:
    row = dict(item or {})
    title = _safe_text(row.get("title"))
    if not title:
        return []
    title_text = _markdown_link(_compact_homepage_evidence_text(title) or title, row.get("link"))
    detail = _safe_text(row.get("lead_detail")) or _safe_text(row.get("layer")) or "相关情报"
    if detail.startswith("主题事件："):
        detail = _compact_event_label(detail) or detail
    signal_type = _safe_text(row.get("signal_type")) or detail
    if signal_type.startswith("主题事件："):
        signal_type = _compact_event_label(signal_type) or signal_type
    lines = [f"- 相关情报补充：`{signal_type}`。{title_text}"]
    signal_parts = [
        f"信号类型：`{signal_type}`",
        f"强弱：`{_safe_text(row.get('signal_strength')) or _safe_text(row.get('importance_label')) or '中'}`",
    ]
    conclusion = _safe_text(row.get("signal_conclusion"))
    if conclusion:
        signal_parts.append(f"结论：{conclusion}")
    lines.append("  " + _ensure_sentence_ending("；".join(signal_parts)))
    source_parts = []
    source = _safe_text(row.get("source"))
    date = _safe_text(row.get("date"))
    if source or date:
        source_parts.append("来源：" + " / ".join(part for part in (source, date) if part))
    tags = list(row.get("intelligence_attributes") or [])
    if tags:
        source_parts.append(f"情报属性：`{format_intelligence_attributes(tags)}`")
    source_lane = _safe_text(row.get("source_lane"))
    if source_lane:
        source_parts.append(f"来源层级：`{source_lane}`")
    if source_parts:
        lines.append("  " + _ensure_sentence_ending("；".join(source_parts)))
    return lines


def _related_item_section_line(item: Mapping[str, Any]) -> str:
    return "\n".join(_related_item_section_lines(item))


def _append_section_block(lines: List[str], block: Sequence[str]) -> None:
    clean_block = [str(item) for item in block if _safe_text(item)]
    if not clean_block:
        return
    if lines and lines[-1] != "":
        lines.append("")
    lines.extend(clean_block)


def _ensure_sentence_ending(text: Any) -> str:
    value = _safe_text(text)
    if not value:
        return ""
    if value.endswith(("。", "！", "？")):
        return value
    return f"{value}。"


def _no_external_intelligence_line() -> str:
    return "当前可前置的外部情报仍偏少，先把主题逻辑和后文证据合在一起理解。"


def event_digest_homepage_lines(payload: Mapping[str, Any], fallback_lines: Sequence[str] | None = None) -> List[str]:
    digest = dict(payload or {})
    fallback = [_safe_text(item) for item in list(fallback_lines or []) if _safe_text(item)]
    if not digest:
        return fallback[:2]
    as_of = _safe_text(digest.get("latest_signal_at")) or _safe_text(digest.get("previous_reviewed_at"))
    as_of = _safe_text(digest.get("as_of")) or as_of
    status = _safe_text(digest.get("status"))
    changed_what = _safe_text(digest.get("changed_what"))
    lead_layer = _safe_text(digest.get("lead_layer"))
    lead_detail = _safe_text(digest.get("lead_detail"))
    lead_title = _safe_text(digest.get("lead_title"))
    lead_link = _safe_text(digest.get("lead_link"))
    impact_summary = _safe_text(digest.get("impact_summary"))
    thesis_scope = _safe_text(digest.get("thesis_scope"))
    compact_reason = compact_importance_reason(digest.get("importance_reason"))
    history_note = _safe_text(digest.get("history_note"))
    previous_reviewed_at = _safe_text(digest.get("previous_reviewed_at"))
    review_history_text = review_history_context_text(digest)
    related_items = _related_event_items(digest, limit=3)
    lead_tags = list(digest.get("intelligence_attributes") or [])
    if not lead_tags:
        lead_item = dict((list(digest.get("items") or []) or [{}])[0] or {})
        lead_tags = intelligence_attribute_labels(
            {
                **lead_item,
                "layer": _safe_text(lead_item.get("raw_layer")) or _safe_text(lead_item.get("layer")) or lead_layer,
                "lead_detail": lead_detail,
            },
            as_of=as_of,
            first_tracking=not previous_reviewed_at,
            previous_reviewed_at=previous_reviewed_at,
        )
    source_lane = intelligence_source_lane(lead_tags)
    signal_type = _safe_text(digest.get("signal_type")) or lead_detail or lead_layer
    signal_strength = _safe_text(digest.get("signal_strength")) or _safe_text(digest.get("importance_label")) or "中"
    signal_conclusion = _safe_text(digest.get("signal_conclusion"))
    compact_label = _compact_event_label(lead_detail or signal_type or lead_layer) or _safe_text(signal_type) or "事件"
    lines: List[str] = []
    summary_parts: List[str] = []
    if status in _STATUS_VALUES:
        compact_change = _compact_changed_what_text(changed_what, impact_summary=impact_summary, thesis_scope=thesis_scope)
        summary_parts.append(f"事件状态 `{status}`" + (f"：{compact_change}" if compact_change else "。"))
    if history_note:
        summary_parts.append(history_note)
    elif previous_reviewed_at:
        summary_parts.append(review_history_text)
    elif not previous_reviewed_at and not (lead_layer and lead_title and fallback):
        summary_parts.append("这是首次跟踪，当前先建立情报基线。")
    if summary_parts:
        lines.append(" ".join(part.rstrip("；") for part in summary_parts if part))
    if lead_layer and lead_title:
        detail = compact_label or lead_layer
        evidence_label = _compact_homepage_evidence_text(lead_title) or lead_title
        evidence_text = _markdown_link(evidence_label, lead_link)
        suffix_parts = []
        compact_reason_text = compact_reason
        if thesis_scope == "一次性噪音" and compact_reason_text:
            compact_reason_text = "先按辅助线索看，不单独升级动作"
        if source_lane:
            suffix_parts.append(f"来源层级：`{source_lane}`")
        signal_display = _safe_text(signal_type)
        if signal_display == lead_detail and lead_detail.startswith("主题事件："):
            signal_display = _compact_event_label(signal_display) or signal_display
        if signal_display:
            suffix_parts.append(f"信号：`{signal_display}`")
        if signal_strength:
            suffix_parts.append(f"信号强弱：`{signal_strength}`")
        if signal_conclusion:
            suffix_parts.append(f"结论：{signal_conclusion}")
        if impact_summary:
            suffix_parts.append(f"更直接影响 `{impact_summary}`")
        if thesis_scope:
            suffix_parts.append(f"当前更像 `{thesis_scope}`")
        if compact_reason_text:
            suffix_parts.append(compact_reason_text)
        lead_prefix = "先看"
        if thesis_scope == "历史基线":
            lead_prefix = "当前先放在背景参考位的是"
        lead_line = f"{lead_prefix} `{detail}`"
        if lead_tags:
            lead_line += f"（情报属性：{format_intelligence_attributes(lead_tags)}）"
        lead_line += f"：{evidence_text}"
        if suffix_parts:
            lead_line += "；" + "；".join(suffix_parts)
        lines.append(lead_line)
    elif lead_layer and status == "待补充":
        fallback_added = False
        for item in fallback:
            if item in lines:
                continue
            lines.append(item)
            fallback_added = True
            break
        if not fallback_added:
            lines.append(_no_external_intelligence_line())
        if related_items:
            lines.append(f"当前更像 `{lead_layer}` 层的补证据阶段，不把空白直接写成已经验证。")
    for item in related_items:
        if len(lines) >= 3:
            break
        line = _related_item_homepage_line(item)
        if not line or line in lines:
            continue
        lines.append(line)
    for item in fallback:
        if item in lines:
            continue
        lines.append(item)
        if len(lines) >= 3:
            break
    return lines[:3]


def event_digest_action_line(payload: Mapping[str, Any], *, observe_only: bool = False) -> str:
    digest = dict(payload or {})
    if not digest:
        return ""
    status = _safe_text(digest.get("status"))
    lead_layer = _safe_text(digest.get("lead_layer")) or "事件"
    lead_detail = _safe_text(digest.get("lead_detail")) or lead_layer
    compact_label = lead_detail
    if lead_detail.startswith("主题事件："):
        compact_label = _compact_event_label(lead_detail) or _compact_event_label(digest.get("signal_type")) or lead_layer
    impact_summary = _safe_text(digest.get("impact_summary"))
    thesis_scope = _safe_text(digest.get("thesis_scope"))
    compact_reason = compact_importance_reason(digest.get("importance_reason"))
    next_step = _safe_text(digest.get("next_step"))
    history_note = _safe_text(digest.get("history_note"))
    if status == "待复核":
        review_reason = compact_reason or f"先补 `{compact_label}` 的复核，再决定能不能升级动作"
        return f"事件先按 `{status}` 处理：{review_reason}。"
    if history_note and "暂无" in history_note:
        return f"{history_note} {next_step or '先别把旧闻回放直接升级成新动作。'}"
    if thesis_scope == "一次性噪音":
        boundary_reason = "先按辅助线索看，不单独升级动作" if compact_reason else "先别因为标题直接改动作"
        return (
            f"这次 `{compact_label}` 更像 `一次性噪音`，{boundary_reason}；"
            f"{next_step or '继续看价格确认和后续证据。'}"
        )
    if thesis_scope == "thesis变化":
        if observe_only:
            reason_suffix = f"，{compact_reason}" if compact_reason else ""
            return (
                f"这次 `{compact_label}` 已开始改写 `{impact_summary or '研究锚'}`{reason_suffix}，"
                f"动作上先等 `{next_step or '价格确认'}` 回来。"
            )
        reason_suffix = f"，{compact_reason}" if compact_reason else ""
        return (
            f"这次 `{compact_label}` 更像 `thesis变化`：它已经开始改写 `{impact_summary or '研究锚'}`{reason_suffix}。"
            f"动作上先按 `{next_step or '兑现节奏和价格确认'}` 走。"
        )
    observation_reason = compact_reason or "先别因为一条标题直接改动作"
    return f"当前先把 `{compact_label}` 当观察线索：{observation_reason}。{next_step or '继续补执行细节。'}"


def render_event_digest_section(payload: Mapping[str, Any], *, heading: str = "## 事件消化") -> List[str]:
    digest = dict(payload or {})
    if not digest:
        return []
    status = _safe_text(digest.get("status"))
    lead_layer = _safe_text(digest.get("lead_layer"))
    lead_detail = _safe_text(digest.get("lead_detail"))
    importance_label = _safe_text(digest.get("importance_label")) or _IMPORTANCE_LABELS.get(_safe_text(digest.get("importance")), "中")
    changed_what = _safe_text(digest.get("changed_what"))
    lead_title = _safe_text(digest.get("lead_title"))
    lead_link = _safe_text(digest.get("lead_link"))
    impact_summary = _safe_text(digest.get("impact_summary"))
    thesis_scope = _safe_text(digest.get("thesis_scope"))
    importance_reason = _safe_text(digest.get("importance_reason"))
    next_step = _safe_text(digest.get("next_step"))
    history_note = _safe_text(digest.get("history_note"))
    latest_signal_at = _safe_text(digest.get("latest_signal_at"))
    previous_reviewed_at = _safe_text(digest.get("previous_reviewed_at"))
    related_items = _related_event_items(digest, limit=3)
    lead_tags = list(digest.get("intelligence_attributes") or [])
    if not lead_tags:
        lead_item = dict((list(digest.get("items") or []) or [{}])[0] or {})
        lead_tags = intelligence_attribute_labels(
            {
                **lead_item,
                "layer": _safe_text(lead_item.get("raw_layer")) or _safe_text(lead_item.get("layer")) or lead_layer,
                "lead_detail": lead_detail,
            },
            as_of=_safe_text(digest.get("as_of")) or latest_signal_at or previous_reviewed_at,
            first_tracking=not previous_reviewed_at,
            previous_reviewed_at=previous_reviewed_at,
        )
    lines = [heading, ""]
    overview_block = [
        f"- 事件状态：`{status or '待补充'}`。",
        f"- 事件分层：`{lead_layer or '新闻'}`；优先级：`{importance_label}`。",
    ]
    compact_detail = lead_detail
    if lead_detail.startswith("主题事件："):
        compact_detail = _compact_event_label(lead_detail) or lead_detail
    if history_note:
        overview_block.append(f"- 与上次复查相比：{history_note}")
    elif not previous_reviewed_at:
        overview_block.append("- 与上次复查相比：这是首次跟踪，先建立情报基线。")
    if previous_reviewed_at:
        overview_block.append(f"- 上次复查时间：{previous_reviewed_at}")
    if compact_detail and lead_detail and compact_detail != lead_detail:
        overview_block.append(f"- 事件细分：先按 `{compact_detail}` 理解（原始合同：`{lead_detail}`）。")
    elif lead_detail:
        overview_block.append(f"- 事件细分：`{lead_detail}`。")
    elif compact_detail:
        overview_block.append(f"- 事件细分：`{compact_detail}`。")
    _append_section_block(lines, overview_block)
    signal_type = _safe_text(digest.get("signal_type")) or compact_detail or lead_layer
    if signal_type == lead_detail and lead_detail.startswith("主题事件："):
        signal_type = compact_detail or signal_type
    signal_strength = _safe_text(digest.get("signal_strength")) or importance_label
    signal_conclusion = _safe_text(digest.get("signal_conclusion"))
    signal_parts = []
    if signal_type:
        signal_parts.append(f"信号类型：`{signal_type}`")
    if signal_strength:
        signal_parts.append(f"信号强弱：`{signal_strength}`")
    if signal_conclusion:
        signal_parts.append(f"结论：{signal_conclusion}")
    if signal_parts:
        _append_section_block(lines, [f"- 信号判断：{_ensure_sentence_ending('；'.join(signal_parts))}"])
    if lead_tags:
        attrs_block = [f"- 情报属性：`{format_intelligence_attributes(lead_tags)}`。"]
        source_lane = intelligence_source_lane(lead_tags)
        if source_lane:
            attrs_block.append(f"- 来源层级：`{source_lane}`。")
        _append_section_block(lines, attrs_block)
    if latest_signal_at:
        _append_section_block(lines, [f"- 最新情报时点：{latest_signal_at}"])
    if impact_summary or thesis_scope:
        detail_parts = []
        if impact_summary:
            detail_parts.append(f"更直接影响 `{impact_summary}`")
        if thesis_scope:
            detail_parts.append(f"当前更像 `{thesis_scope}`")
        _append_section_block(lines, [f"- 影响层与性质：{'；'.join(detail_parts)}。"])
    if importance_reason:
        _append_section_block(
            lines,
            [f"- 优先级判断：{_compact_priority_reason_text(importance_reason, lead_detail=lead_detail, thesis_scope=thesis_scope)}"],
        )
    if changed_what:
        _append_section_block(lines, [f"- 这件事改变了什么：{_compact_changed_what_text(changed_what, impact_summary=impact_summary, thesis_scope=thesis_scope)}"])
    if lead_title:
        _append_section_block(lines, [f"- 当前前置事件：{_markdown_link(_compact_homepage_evidence_text(lead_title) or lead_title, lead_link)}"])
    related_blocks: List[str] = []
    for item in related_items:
        item_lines = _related_item_section_lines(item)
        if not item_lines:
            continue
        if related_blocks:
            related_blocks.append("")
        related_blocks.extend(item_lines)
    _append_section_block(lines, related_blocks)
    if next_step:
        _append_section_block(lines, [f"- 现在更该做什么：{next_step}"])
    return lines
