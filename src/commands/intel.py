"""Ad-hoc intelligence collection without final/report guard workflows."""

from __future__ import annotations

import argparse
import io
import re
from copy import deepcopy
from contextlib import redirect_stderr
from datetime import datetime
from collections import defaultdict
from typing import Any, Callable, Dict, List, Mapping, Sequence

from src.collectors import AssetLookupCollector, NewsCollector
from src.utils.config import load_config
from src.utils.logger import logger


PREFERRED_SOURCES = ["财联社", "证券时报", "上海证券报", "中国证券报", "Reuters", "Bloomberg"]
THEME_NOISE_TITLE_MARKERS = (
    "个股诊断",
    "关注支撑位",
    "股价加速下跌",
    "板块走弱",
    "冲高回落",
    "估值偏高",
    "止损参考",
    "目标参考",
)
SOURCE_TIER_LABELS = {
    "first_party": "一手/官方",
    "structured": "结构化",
    "primary_media": "主流媒体",
    "industry_source": "行业/协会",
    "aggregator": "聚合转述",
    "search_fallback": "搜索补充",
}
SOURCE_TIER_RANK = {
    "first_party": 6,
    "structured": 5,
    "primary_media": 4,
    "industry_source": 3,
    "aggregator": 2,
    "search_fallback": 1,
}
SOURCE_TIER_MARKERS = {
    "first_party": (
        "CNINFO",
        "巨潮资讯",
        "上交所",
        "深交所",
        "SSE",
        "SZSE",
        "Investor Relations",
        "投资者关系",
        "互动易",
        "e互动",
        "业绩说明会",
        "路演纪要",
        "官网",
    ),
    "primary_media": (
        "财联社",
        "证券时报",
        "上海证券报",
        "中国证券报",
        "Reuters",
        "Bloomberg",
        "第一财经",
        "华尔街见闻",
    ),
    "industry_source": (
        "中国有色网",
        "集微网",
        "协会",
        "硅业分会",
        "中物联",
    ),
    "aggregator": (
        "同花顺",
        "新浪财经",
        "东方财富",
        "财富号",
        "证券之星",
        "雪球",
    ),
}
THEME_BUCKET_RULES = (
    ("价格/供需", ("价格", "涨价", "铜价", "铝价", "金价", "镍价", "锂价", "供给", "需求", "库存", "排产", "开工", "供需", "扰动", "景气")),
    ("政策/宏观", ("政策", "通知", "规划", "方案", "会议", "国务院", "发改委", "财政", "央行", "地缘", "关税", "出口", "进口")),
    ("资金/交易", ("资金", "主力", "净申购", "净流入", "ETF", "换手", "活跃", "成交", "涨停", "游资", "持仓")),
    ("产业/公司", ("调研", "协会", "公司", "合作", "订单", "中标", "财报", "业绩", "交流会", "技术", "转型", "项目", "扩产")),
    ("会议/活动", ("展会", "大会", "论坛", "发布会", "峰会", "活动", "博览会", "交流会")),
)
INTEL_CLUSTER_STOPWORDS = {
    "a股",
    "港股",
    "美股",
    "市场",
    "板块",
    "主题",
    "相关",
    "情报",
    "催化",
    "线索",
    "行业",
    "今日",
    "最近",
    "中国",
}
MAJOR_EVENT_QUERY_RULES = (
    (
        ("停火", "休战", "ceasefire", "truce", "伊朗", "以色列", "中东", "国际局势", "能源与地缘", "风险偏好"),
        (
            "美伊 停火 中东 风险偏好 财联社 Reuters",
            "伊朗 以色列 停火 休战 原油 黄金 财联社 Reuters",
        ),
    ),
    (
        ("降息", "加息", "fed", "美联储", "央行", "收益率", "yield", "利率"),
        (
            "美联储 降息 利率 收益率 风险偏好 财联社 Reuters",
            "中国 央行 利率 货币政策 流动性 财联社 证券时报",
        ),
    ),
    (
        ("关税", "贸易战", "制裁", "出口管制", "tariff", "sanction", "export control"),
        (
            "关税 制裁 出口管制 风险偏好 财联社 Reuters",
            "贸易战 出口管制 产业链 财联社 证券时报",
        ),
    ),
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Collect ad-hoc market/theme intelligence without final delivery gates.")
    parser.add_argument("query", nargs="+", help="Theme, asset or natural-language intelligence request")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--symbol", default="", help="Optional explicit symbol to anchor structured intelligence")
    parser.add_argument("--limit", type=int, default=8, help="Maximum number of surfaced intelligence items")
    parser.add_argument("--recent-days", type=int, default=7, help="Recent-day window for candidate filtering")
    parser.add_argument(
        "--structured-only",
        action="store_true",
        help="Only use structured intelligence lanes and skip RSS/topic search backfill",
    )
    return parser


def _normalize_query_terms(query: str) -> List[str]:
    raw_query = str(query).strip()
    cleaned_query = re.sub(r"(帮我|给我|收集|整理|相关的|相关|情报|催化|线索|最近|看看|看一下|一下|一下子)", " ", raw_query)
    cleaned_query = re.sub(r"\s+", " ", cleaned_query).strip(" ，。；;、")
    cleaned_query = cleaned_query or raw_query
    if not cleaned_query:
        return []
    pieces = [token.strip() for token in re.split(r"[\s,，。；;、/|]+", cleaned_query) if token.strip()]
    terms: List[str] = [cleaned_query]
    for piece in pieces:
        if len(piece) <= 1:
            continue
        if piece not in terms:
            terms.append(piece)
    return terms[:6]


def _build_query_groups(query: str) -> List[List[str]]:
    terms = _normalize_query_terms(query)
    groups: List[List[str]] = []
    if terms:
        groups.append([terms[0]])
    if len(terms) >= 2:
        groups.append(terms[:2])
        groups.append([terms[0], terms[1], "A股"])
    if len(terms) >= 3:
        groups.append(terms[:3])
    if terms:
        groups.append([terms[0], "行业"])
        groups.append([terms[0], "板块"])
    deduped: List[List[str]] = []
    seen: set[tuple[str, ...]] = set()
    for group in groups:
        normalized = tuple(str(item).strip() for item in group if str(item).strip())
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        deduped.append(list(normalized))
    return deduped[:6]


def _should_attempt_symbol_resolution(query: str) -> bool:
    text = str(query).strip()
    if not text:
        return False
    if re.search(r"\b[A-Z]{1,5}\b|(?<!\d)\d{5,6}(?!\d)|\b[A-Z]{1,2}\d\b", text.upper()):
        return True
    keywords = ("ETF", "基金", "指数", "股票", "个股", "标的", "代码")
    return any(keyword in text for keyword in keywords)


def _dedupe_items(items: Sequence[Mapping[str, Any]]) -> List[Dict[str, str]]:
    deduped: List[Dict[str, str]] = []
    seen: set[tuple[str, str, str]] = set()
    for item in items:
        if not isinstance(item, Mapping):
            continue
        title = str(item.get("title") or "").strip()
        source = str(item.get("source") or item.get("configured_source") or "").strip()
        link = str(item.get("link") or "").strip()
        if not title:
            continue
        key = (title, source, link)
        if key in seen:
            continue
        seen.add(key)
        deduped.append({str(k): str(v) if v is not None else "" for k, v in item.items()})
    return deduped


def _normalized_title_key(title: str) -> str:
    cleaned = str(title).strip().lower()
    if not cleaned:
        return ""
    cleaned = re.sub(r"\s*[-|·]\s*(reuters|bloomberg|财联社|证券时报|同花顺|新浪财经|东方财富|财富号)\s*$", "", cleaned)
    cleaned = re.sub(r"[^0-9a-z\u4e00-\u9fff]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def _title_signature_tokens(title: str, query_terms: Sequence[str]) -> set[str]:
    text = _normalized_title_key(title)
    if not text:
        return set()
    query_tokens = {
        token
        for token in re.split(r"\s+", " ".join(str(term).strip().lower() for term in query_terms if str(term).strip()))
        if len(token) >= 2
    }
    tokens = {
        token
        for token in re.findall(r"[0-9a-z]{2,}|[\u4e00-\u9fff]{2,}", text)
        if token not in INTEL_CLUSTER_STOPWORDS and token not in query_tokens
    }
    return tokens


def _item_timestamp(item: Mapping[str, Any]) -> datetime | None:
    for key in ("published_at", "date", "as_of"):
        text = str(item.get(key) or "").strip()
        if not text:
            continue
        try:
            value = datetime.fromisoformat(text.replace("Z", "+00:00"))
        except ValueError:
            try:
                value = datetime.strptime(text[:19], "%Y-%m-%d %H:%M:%S")
            except ValueError:
                try:
                    value = datetime.strptime(text[:10], "%Y-%m-%d")
                except ValueError:
                    continue
        if getattr(value, "tzinfo", None) is not None:
            value = value.replace(tzinfo=None)
        return value
    return None


def _source_tier(item: Mapping[str, Any]) -> str:
    source = str(item.get("source") or item.get("configured_source") or "").strip()
    category = str(item.get("category") or "").strip().lower()
    haystack = f"{source} {category}".strip().lower()
    if any(str(marker).lower() in haystack for marker in SOURCE_TIER_MARKERS["first_party"]):
        return "first_party"
    if category in {"stk_surv", "structured", "irm_qa_sh", "irm_qa_sz", "stock_news"}:
        return "structured"
    if any(str(marker).lower() in haystack for marker in SOURCE_TIER_MARKERS["primary_media"]):
        return "primary_media"
    if any(str(marker).lower() in haystack for marker in SOURCE_TIER_MARKERS["industry_source"]):
        return "industry_source"
    if any(str(marker).lower() in haystack for marker in SOURCE_TIER_MARKERS["aggregator"]):
        return "aggregator"
    if category in {"topic_search", "rss", "feed"}:
        return "search_fallback"
    return "aggregator"


def _news_report_sufficient(report: Mapping[str, Any] | None) -> bool:
    items = [dict(item) for item in list(dict(report or {}).get("items") or []) if isinstance(item, Mapping)]
    linked_items = [item for item in items if str(item.get("link") or "").strip()]
    return len(linked_items) >= 2 or (len(items) >= 3 and len(linked_items) >= 1)


def _market_hint_blob(
    query: str,
    *,
    baseline_report: Mapping[str, Any] | None = None,
    market_event_rows: Sequence[Sequence[Any]] | None = None,
    hint_lines: Sequence[str] | None = None,
) -> str:
    parts: List[str] = [str(query).strip()]
    report = dict(baseline_report or {})
    parts.extend(str(item).strip() for item in list(report.get("summary_lines") or []) if str(item).strip())
    parts.extend(str(item).strip() for item in list(report.get("lines") or []) if str(item).strip())
    note = str(report.get("note") or "").strip()
    if note:
        parts.append(note)
    for item in list(report.get("items") or []):
        if not isinstance(item, Mapping):
            continue
        for key in ("title", "category", "source", "configured_source", "note", "signal_type", "signal_conclusion"):
            text = str(item.get(key) or "").strip()
            if text:
                parts.append(text)
    for row in list(market_event_rows or []):
        for value in row[:8]:
            text = str(value or "").strip()
            if text:
                parts.append(text)
    parts.extend(str(item).strip() for item in list(hint_lines or []) if str(item).strip())
    return " ".join(parts).lower()


def _priority_market_queries(
    query: str,
    *,
    baseline_report: Mapping[str, Any] | None = None,
    market_event_rows: Sequence[Sequence[Any]] | None = None,
    hint_lines: Sequence[str] | None = None,
) -> List[str]:
    blob = _market_hint_blob(
        query,
        baseline_report=baseline_report,
        market_event_rows=market_event_rows,
        hint_lines=hint_lines,
    )
    prioritized: List[str] = []
    for trigger_tokens, queries in MAJOR_EVENT_QUERY_RULES:
        if not any(token.lower() in blob for token in trigger_tokens):
            continue
        for item in queries:
            text = str(item).strip()
            if text and text not in prioritized:
                prioritized.append(text)
    return prioritized


def _theme_bucket(item: Mapping[str, Any], *, query_terms: Sequence[str]) -> str:
    title = str(item.get("title") or "").strip()
    category = str(item.get("category") or "").strip()
    source = str(item.get("source") or "").strip()
    haystack = " ".join([title, category, source, *[str(term).strip() for term in query_terms if str(term).strip()]])
    for label, tokens in THEME_BUCKET_RULES:
        if any(token in haystack for token in tokens):
            return label
    return "综合/其他"


def _representative_rank(item: Mapping[str, Any], *, query_terms: Sequence[str]) -> tuple[int, int, int, int]:
    title = str(item.get("title") or "").strip()
    source_tier = str(item.get("source_tier") or _source_tier(item))
    tier_rank = SOURCE_TIER_RANK.get(source_tier, 0)
    timestamp = _item_timestamp(item)
    freshness_rank = int(timestamp.timestamp()) if timestamp else 0
    query_anchor_rank = sum(1 for term in query_terms if len(str(term).strip()) >= 2 and str(term).strip() in title)
    category = str(item.get("category") or "").strip().lower()
    category_rank = {
        "stk_surv": 4,
        "stock_news": 3,
        "market_intelligence": 2,
        "topic_search": 1,
    }.get(category, 0)
    return (tier_rank, query_anchor_rank, category_rank, freshness_rank)


def _cluster_items(items: Sequence[Mapping[str, Any]], *, query_terms: Sequence[str]) -> List[Dict[str, Any]]:
    clusters: List[Dict[str, Any]] = []
    for raw in items:
        if not isinstance(raw, Mapping):
            continue
        item = dict(raw)
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        item["source_tier"] = _source_tier(item)
        item["source_tier_label"] = SOURCE_TIER_LABELS.get(str(item["source_tier"]), "补充")
        item["theme_bucket"] = _theme_bucket(item, query_terms=query_terms)
        title_key = _normalized_title_key(title)
        tokens = _title_signature_tokens(title, query_terms)
        assigned = None
        for cluster in clusters:
            if title_key and title_key == cluster["title_key"]:
                assigned = cluster
                break
            overlap = tokens & set(cluster["tokens"])
            if len(overlap) >= 2:
                assigned = cluster
                break
        if assigned is None:
            assigned = {
                "title_key": title_key,
                "tokens": set(tokens),
                "items": [],
            }
            clusters.append(assigned)
        assigned["items"].append(item)
        assigned["tokens"] = set(assigned["tokens"]) | set(tokens)

    normalized_clusters: List[Dict[str, Any]] = []
    for cluster in clusters:
        cluster_items = list(cluster["items"])
        representative = max(
            cluster_items,
            key=lambda item: _representative_rank(item, query_terms=query_terms),
        )
        sources = sorted(
            {
                str(item.get("source") or item.get("configured_source") or "").strip()
                for item in cluster_items
                if str(item.get("source") or item.get("configured_source") or "").strip()
            }
        )
        normalized_clusters.append(
            {
                "representative": dict(representative),
                "items": [dict(item) for item in cluster_items],
                "cluster_size": len(cluster_items),
                "sources": sources,
                "theme_bucket": str(representative.get("theme_bucket") or "综合/其他"),
                "source_tier": str(representative.get("source_tier") or "search_fallback"),
            }
        )
    normalized_clusters.sort(
        key=lambda cluster: (
            _representative_rank(cluster["representative"], query_terms=query_terms),
            cluster["cluster_size"],
        ),
        reverse=True,
    )
    return normalized_clusters


def _select_cluster_representatives(
    clusters: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> List[Dict[str, Any]]:
    bucketed: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for cluster in clusters:
        representative = dict(cluster.get("representative") or {})
        if not representative:
            continue
        representative["cluster_size"] = int(cluster.get("cluster_size") or 1)
        representative["cluster_sources"] = list(cluster.get("sources") or [])
        representative["theme_bucket"] = str(cluster.get("theme_bucket") or representative.get("theme_bucket") or "综合/其他")
        representative["source_tier"] = str(cluster.get("source_tier") or representative.get("source_tier") or "search_fallback")
        representative["source_tier_label"] = SOURCE_TIER_LABELS.get(str(representative["source_tier"]), "补充")
        bucketed[str(representative.get("theme_bucket") or "综合/其他")].append(representative)

    selected: List[Dict[str, Any]] = []
    seen_titles: set[str] = set()
    for bucket in sorted(bucketed.keys()):
        if len(selected) >= limit:
            break
        representative = bucketed[bucket][0]
        title = str(representative.get("title") or "").strip()
        if title and title not in seen_titles:
            selected.append(representative)
            seen_titles.add(title)

    if len(selected) < limit:
        remainder = [
            representative
            for bucket in sorted(bucketed.keys())
            for representative in bucketed[bucket]
            if str(representative.get("title") or "").strip() not in seen_titles
        ]
        for representative in remainder:
            if len(selected) >= limit:
                break
            title = str(representative.get("title") or "").strip()
            if title and title not in seen_titles:
                selected.append(representative)
                seen_titles.add(title)
    return selected[:limit]


def _group_cluster_representatives(items: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    bucketed: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for item in items:
        bucketed[str(item.get("theme_bucket") or "综合/其他")].append(dict(item))
    groups: List[Dict[str, Any]] = []
    for bucket, bucket_items in sorted(bucketed.items(), key=lambda item: (-len(item[1]), item[0])):
        groups.append(
            {
                "label": bucket,
                "count": len(bucket_items),
                "items": bucket_items,
            }
        )
    return groups


def _source_tier_rows(items: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    counts: Dict[str, int] = defaultdict(int)
    for item in items:
        tier = str(item.get("source_tier") or "search_fallback")
        counts[tier] += 1
    rows: List[Dict[str, Any]] = []
    for tier, count in sorted(counts.items(), key=lambda item: (-SOURCE_TIER_RANK.get(item[0], 0), -item[1], item[0])):
        rows.append(
            {
                "tier": tier,
                "label": SOURCE_TIER_LABELS.get(tier, tier),
                "count": count,
            }
        )
    return rows


def _summary_lines_from_payload(payload: Mapping[str, Any]) -> List[str]:
    grouped_items = [dict(item) for item in list(payload.get("grouped_items") or []) if isinstance(item, Mapping)]
    source_tiers = [dict(item) for item in list(payload.get("source_tiers") or []) if isinstance(item, Mapping)]
    lines: List[str] = []
    if grouped_items:
        parts = [
            f"{str(group.get('label') or '综合/其他').strip()} {int(group.get('count') or 0)} 条"
            for group in grouped_items[:3]
            if str(group.get("label") or "").strip()
        ]
        if parts:
            lines.append("主题聚类：" + "，".join(parts))
    if source_tiers:
        parts = [
            f"{str(row.get('label') or row.get('tier') or '补充').strip()} {int(row.get('count') or 0)} 条"
            for row in source_tiers[:3]
            if str(row.get("label") or row.get("tier") or "").strip()
        ]
        if parts:
            lines.append("来源分层：" + "，".join(parts))
    lead_item = dict(next(iter(list(payload.get("items") or [])), {}) or {})
    lead_bucket = str(lead_item.get("theme_bucket") or "").strip()
    lead_tier = str(lead_item.get("source_tier_label") or "").strip()
    if lead_bucket or lead_tier:
        parts = [part for part in (lead_bucket, lead_tier) if part]
        lines.append("当前更值得先看的代表情报来自：" + " / ".join(parts))
    return lines[:3]


def _theme_noise_score(item: Mapping[str, Any], *, query_terms: Sequence[str]) -> int:
    title = str(item.get("title") or "").strip()
    if not title:
        return 10
    lowered_title = title.lower()
    score = 0
    if any(marker in title for marker in THEME_NOISE_TITLE_MARKERS):
        score += 5
    if "：" in title and title.count("：") >= 2:
        score += 2
    if "；" in title or ";" in title:
        score += 1
    if len(title) >= 55:
        score += 1
    compact_terms = [str(term).strip() for term in query_terms if len(str(term).strip()) >= 2]
    if compact_terms and not any(term in title for term in compact_terms):
        score += 3
    if "topic_search" in lowered_title and "google" in lowered_title:
        score += 1
    return score


def _prune_theme_noise_items(
    items: Sequence[Mapping[str, Any]],
    *,
    query_terms: Sequence[str],
    symbol: str = "",
    asset_name: str = "",
) -> List[Dict[str, str]]:
    if symbol or asset_name:
        return [dict(item) for item in items if isinstance(item, Mapping)]
    query_tokens = [str(term).strip() for term in query_terms if str(term).strip()]
    filtered: List[Dict[str, str]] = []
    for item in items:
        if not isinstance(item, Mapping):
            continue
        title = str(item.get("title") or "").strip()
        if not title:
            continue
        score = _theme_noise_score(item, query_terms=query_tokens)
        if score >= 5:
            continue
        filtered.append(dict(item))
    return filtered if filtered else [dict(item) for item in items if isinstance(item, Mapping)]


def collect_intel_payload(
    query: str,
    *,
    config: Mapping[str, Any],
    explicit_symbol: str = "",
    limit: int = 8,
    recent_days: int = 7,
    structured_only: bool = False,
) -> Dict[str, Any]:
    effective = deepcopy(dict(config or {}))
    if structured_only:
        effective["news_topic_search_enabled"] = False
    else:
        effective["news_topic_search_enabled"] = True
        current_feeds = str(effective.get("news_feeds_file", "") or "").strip()
        if not current_feeds or current_feeds.endswith("news_feeds.empty.yaml"):
            effective["news_feeds_file"] = "config/news_feeds.yaml"

    query_text = str(query).strip()
    query_terms = _normalize_query_terms(query_text)
    query_groups = _build_query_groups(query_text)
    lookup = AssetLookupCollector(effective)
    resolved = {}
    if explicit_symbol:
        resolved = {"symbol": explicit_symbol.strip()}
    elif _should_attempt_symbol_resolution(query_text):
        resolved = dict(lookup.resolve_best(query_text) or {})

    symbol = str(resolved.get("symbol") or "").strip()
    asset_name = str(resolved.get("name") or "").strip()
    asset_type = str(resolved.get("asset_type") or "").strip()

    collector = NewsCollector(effective)
    market_hits: List[Dict[str, str]] = []
    search_hits: List[Dict[str, str]] = []
    stock_hits: List[Dict[str, str]] = []
    surv_snapshot: Dict[str, Any] = {}
    notes: List[str] = []

    try:
        market_hits = list(collector.get_market_intelligence(query_terms, limit=max(limit, 6), recent_days=max(recent_days, 3)))
    except Exception:
        market_hits = []
    if market_hits:
        notes.append("已命中 Tushare 市场情报。")

    if not structured_only and query_groups:
        try:
            search_hits = list(
                collector.search_by_keyword_groups(
                    query_groups,
                    preferred_sources=PREFERRED_SOURCES,
                    limit=max(limit, 6),
                    recent_days=recent_days,
                )
            )
        except Exception:
            search_hits = []
        if search_hits:
            notes.append("已命中 RSS/topic search。")

    if symbol:
        try:
            stock_hits = list(collector.get_stock_news(symbol, limit=max(min(limit, 10), 4)))
        except Exception:
            stock_hits = []
        if stock_hits:
            notes.append("已命中单标的情报。")

        try:
            surv_snapshot = dict(collector.get_stk_surv(symbol, limit=max(min(limit, 10), 4)) or {})
        except Exception:
            surv_snapshot = {}
        if list(surv_snapshot.get("items") or []):
            notes.append("已命中机构调研快照。")

    merged = _dedupe_items([*market_hits, *search_hits, *stock_hits, *list(surv_snapshot.get("items") or [])])
    query_keywords = list(query_terms)
    if symbol:
        query_keywords.append(symbol)
    if asset_name:
        query_keywords.append(asset_name)
    filtered = collector._filter_candidate_items(merged, recent_days=max(recent_days, 3))
    filtered = _prune_theme_noise_items(filtered, query_terms=query_terms, symbol=symbol, asset_name=asset_name)
    ranked = collector._rank_items(
        filtered,
        preferred_sources=PREFERRED_SOURCES + ["Tushare"],
        query_keywords=query_keywords,
    )
    clusters = _cluster_items(ranked, query_terms=query_terms)
    selected = _select_cluster_representatives(clusters, limit=max(1, limit))
    grouped_items = _group_cluster_representatives(selected)
    source_tiers = _source_tier_rows(selected)

    return {
        "query": query_text,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": symbol,
        "asset_name": asset_name,
        "asset_type": asset_type,
        "structured_only": bool(structured_only),
        "recent_days": int(recent_days),
        "limit": int(limit),
        "items": selected,
        "ranked_items": [dict(cluster.get("representative") or {}) for cluster in clusters],
        "raw_items": ranked,
        "grouped_items": grouped_items,
        "source_tiers": source_tiers,
        "cluster_count": len(clusters),
        "source_list": sorted(collector._present_sources(selected)),
        "query_terms": query_terms,
        "query_groups": query_groups,
        "market_hits_count": len(market_hits),
        "search_hits_count": len(search_hits),
        "stock_hits_count": len(stock_hits),
        "structured_hits_count": len(list(surv_snapshot.get("items") or [])),
        "surv_snapshot": surv_snapshot,
        "note": "".join(notes) if notes else "当前可前置情报仍偏薄，先按搜索/结构化缺失处理。",
        "disclosure": "这是一份自由情报快照，不走 final / release_check / report_guard；空表、旧日期或权限失败都按缺失处理，不伪装成 fresh 命中。",
    }


def _payload_from_ranked_items(
    query: str,
    *,
    items: Sequence[Mapping[str, Any]],
    explicit_symbol: str = "",
    limit: int = 8,
    recent_days: int = 7,
    structured_only: bool = False,
    note: str = "",
    disclosure: str = "",
) -> Dict[str, Any]:
    query_text = str(query).strip()
    query_terms = _normalize_query_terms(query_text)
    merged = _dedupe_items(items)
    filtered = _prune_theme_noise_items(merged, query_terms=query_terms, symbol=explicit_symbol, asset_name="")
    clusters = _cluster_items(filtered, query_terms=query_terms)
    selected = _select_cluster_representatives(clusters, limit=max(1, limit))
    grouped_items = _group_cluster_representatives(selected)
    source_tiers = _source_tier_rows(selected)
    ranked_items = [dict(cluster.get("representative") or {}) for cluster in clusters]
    return {
        "query": query_text,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "symbol": str(explicit_symbol).strip(),
        "asset_name": "",
        "asset_type": "",
        "structured_only": bool(structured_only),
        "recent_days": int(recent_days),
        "limit": int(limit),
        "items": selected,
        "ranked_items": ranked_items,
        "raw_items": filtered,
        "grouped_items": grouped_items,
        "source_tiers": source_tiers,
        "cluster_count": len(clusters),
        "source_list": sorted(
            {
                str(item.get("source") or item.get("configured_source") or "").strip()
                for item in selected
                if str(item.get("source") or item.get("configured_source") or "").strip()
            }
        ),
        "query_terms": query_terms,
        "query_groups": _build_query_groups(query_text),
        "market_hits_count": 0,
        "search_hits_count": 0,
        "stock_hits_count": 0,
        "structured_hits_count": 0,
        "surv_snapshot": {},
        "note": note or "当前可前置情报仍偏薄，先按搜索/结构化缺失处理。",
        "disclosure": disclosure or "这是一份自由情报快照，不走 final / release_check / report_guard；空表、旧日期或权限失败都按缺失处理，不伪装成 fresh 命中。",
    }


def build_news_report_from_intel_payload(
    payload: Mapping[str, Any],
    *,
    note_prefix: str = "",
) -> Dict[str, Any]:
    items = [dict(item) for item in list(payload.get("items") or []) if isinstance(item, Mapping)]
    ranked = [dict(item) for item in list(payload.get("ranked_items") or items) if isinstance(item, Mapping)]
    source_list = [str(item).strip() for item in list(payload.get("source_list") or []) if str(item).strip()]
    note = str(payload.get("note") or "").strip()
    prefix = str(note_prefix).strip()
    composed_note = f"{prefix} {note}".strip() if prefix and note else (prefix or note)
    summary_lines = _summary_lines_from_payload(payload)
    return {
        "mode": "live" if items else "proxy",
        "items": items,
        "all_items": ranked,
        "grouped_items": [dict(item) for item in list(payload.get("grouped_items") or []) if isinstance(item, Mapping)],
        "source_tiers": [dict(item) for item in list(payload.get("source_tiers") or []) if isinstance(item, Mapping)],
        "cluster_count": int(payload.get("cluster_count") or len(items)),
        "summary_lines": summary_lines,
        "lead_summary": summary_lines[0] if summary_lines else "",
        "lines": [str(dict(item).get("title") or "").strip() for item in items if str(dict(item).get("title") or "").strip()],
        "source_list": source_list,
        "note": composed_note,
        "disclosure": str(payload.get("disclosure") or "").strip(),
    }


def collect_intel_news_report(
    query: str,
    *,
    config: Mapping[str, Any],
    explicit_symbol: str = "",
    limit: int = 6,
    recent_days: int = 7,
    structured_only: bool = False,
    note_prefix: str = "",
) -> Dict[str, Any]:
    payload = collect_intel_payload(
        query,
        config=config,
        explicit_symbol=explicit_symbol,
        limit=limit,
        recent_days=recent_days,
        structured_only=structured_only,
    )
    return build_news_report_from_intel_payload(payload, note_prefix=note_prefix)


def collect_market_aware_intel_news_report(
    query: str,
    *,
    config: Mapping[str, Any],
    explicit_symbol: str = "",
    baseline_report: Mapping[str, Any] | None = None,
    market_event_rows: Sequence[Sequence[Any]] | None = None,
    hint_lines: Sequence[str] | None = None,
    limit: int = 6,
    recent_days: int = 7,
    structured_only: bool = False,
    note_prefix: str = "",
    collect_fn: Callable[..., Mapping[str, Any]] | None = None,
) -> Dict[str, Any]:
    existing = dict(baseline_report or {})
    if _news_report_sufficient(existing):
        return existing

    loader = collect_fn or collect_intel_news_report
    merged_ranked: List[Dict[str, Any]] = [
        dict(item) for item in list(existing.get("all_items") or existing.get("items") or []) if isinstance(item, Mapping)
    ]
    queries: List[str] = []
    for item in _priority_market_queries(
        query,
        baseline_report=existing,
        market_event_rows=market_event_rows,
        hint_lines=hint_lines,
    ):
        if item not in queries:
            queries.append(item)
    base_query = str(query).strip()
    if base_query and base_query not in queries:
        queries.append(base_query)

    collected_reports: List[Dict[str, Any]] = []
    for item in queries[:3]:
        report = dict(
            loader(
                item,
                config=config,
                explicit_symbol=explicit_symbol,
                limit=limit,
                recent_days=recent_days,
                structured_only=structured_only,
                note_prefix=note_prefix,
            )
            or {}
        )
        if not report:
            continue
        collected_reports.append(report)
        merged_ranked.extend(
            dict(row)
            for row in list(report.get("all_items") or report.get("items") or [])
            if isinstance(row, Mapping)
        )
        if _news_report_sufficient({"items": merged_ranked}):
            break

    if not merged_ranked:
        return existing

    note_parts: List[str] = []
    if queries and queries[0] != base_query:
        note_parts.append("共享 intel 已按市场级大事假设优先补搜。")
    for report in collected_reports:
        text = str(report.get("note") or "").strip()
        if text and text not in note_parts:
            note_parts.append(text)
    ranking_query = base_query or (queries[0] if queries else "")
    if queries and queries[0] != base_query:
        ranking_query = " ".join(
            dict.fromkeys(
                part
                for part in (queries[0], base_query)
                if str(part).strip()
            )
        ).strip()
    payload = _payload_from_ranked_items(
        ranking_query,
        items=merged_ranked,
        explicit_symbol=explicit_symbol,
        limit=limit,
        recent_days=recent_days,
        structured_only=structured_only,
        note=" ".join(note_parts).strip(),
        disclosure=str(existing.get("disclosure") or "").strip(),
    )
    return build_news_report_from_intel_payload(payload, note_prefix=note_prefix)


def _lead_line(payload: Mapping[str, Any]) -> str:
    items = list(payload.get("items") or [])
    if not items:
        return "当前没有收集到足够可前置的情报；这更像覆盖不足或当日命中偏薄，不等于没有事件。"
    source_list = [str(item).strip() for item in list(payload.get("source_list") or []) if str(item).strip()]
    source_fragment = " / ".join(source_list[:4]) if source_list else "多源"
    return f"已收集 `{len(items)}` 条近 `{payload.get('recent_days', 7)}` 日相关情报，当前来源主要覆盖 `{source_fragment}`。"


def _format_item_line(item: Mapping[str, Any]) -> str:
    title = str(item.get("title") or "未命名情报").strip()
    link = str(item.get("link") or "").strip()
    source = str(item.get("source") or item.get("configured_source") or "未知来源").strip()
    published_at = str(item.get("published_at") or "—").strip()
    category = str(item.get("category") or "").strip()
    note = str(item.get("note") or "").strip()
    signal_type = note or category or "情报线索"
    theme_bucket = str(item.get("theme_bucket") or "").strip()
    source_tier_label = str(item.get("source_tier_label") or SOURCE_TIER_LABELS.get(str(item.get("source_tier") or ""), "")).strip()
    title_part = f"[{title}]({link})" if link else title
    line = f"- `{signal_type}` {title_part} | 来源：{source}"
    if source_tier_label:
        line += f"（{source_tier_label}）"
    line += f" | 时间：{published_at}"
    if theme_bucket:
        line += f" | 分组：{theme_bucket}"
    if note and note != signal_type:
        line += f" | 备注：{note}"
    return line


def _render_intel_markdown(payload: Mapping[str, Any]) -> str:
    query = str(payload.get("query") or "").strip()
    symbol = str(payload.get("symbol") or "").strip()
    asset_name = str(payload.get("asset_name") or "").strip()
    asset_type = str(payload.get("asset_type") or "").strip()
    lines: List[str] = [
        "# 情报快照",
        "",
        f"- 查询：{query}",
        f"- 生成时间：{str(payload.get('generated_at') or '—').strip()}",
        f"- 模式：`{'structured-only' if bool(payload.get('structured_only')) else 'search-first'}`",
        f"- 数据窗口：近 `{payload.get('recent_days', 7)}` 天",
    ]
    if symbol:
        symbol_line = f"- 锚定标的：`{symbol}`"
        if asset_name:
            symbol_line += f" / {asset_name}"
        if asset_type:
            symbol_line += f" (`{asset_type}`)"
        lines.append(symbol_line)
    else:
        lines.append("- 锚定标的：当前未锁定单一标的，按主题/关键词情报处理。")

    lines.extend(
        [
            "",
            "## 一句话",
            _lead_line(payload),
            f"当前已压缩成 `{payload.get('cluster_count', 0)}` 个情报簇，优先前置每簇代表情报，避免同一件事重复刷屏。",
            "",
            "## 关键情报",
        ]
    )
    items = list(payload.get("items") or [])
    if items:
        lines.extend(_format_item_line(item) for item in items)
    else:
        lines.append("- 当前没有足够可前置的情报线索。")

    surv_snapshot = dict(payload.get("surv_snapshot") or {})
    surv_items = list(surv_snapshot.get("items") or [])
    if surv_items:
        lines.extend(["", "## 结构化辅助"])
        lines.extend(_format_item_line(item) for item in surv_items[:3])

    grouped_items = [dict(item) for item in list(payload.get("grouped_items") or []) if isinstance(item, Mapping)]
    if grouped_items:
        lines.extend(["", "## 主题分组"])
        for group in grouped_items:
            label = str(group.get("label") or "综合/其他").strip()
            count = int(group.get("count") or len(list(group.get("items") or [])) or 0)
            lines.append(f"- `{label}`：{count} 条代表情报")
            sample_items = list(group.get("items") or [])[:2]
            for item in sample_items:
                title = str(dict(item).get("title") or "").strip()
                if title:
                    lines.append(f"  - {title}")

    source_tiers = [dict(item) for item in list(payload.get("source_tiers") or []) if isinstance(item, Mapping)]
    if source_tiers:
        lines.extend(["", "## 来源分层"])
        for row in source_tiers:
            lines.append(f"- `{str(row.get('label') or row.get('tier') or '补充').strip()}`：{int(row.get('count') or 0)} 条")

    lines.extend(
        [
            "",
            "## 方法与边界",
            f"- 查询词：`{' / '.join(list(payload.get('query_terms') or [])) or query}`",
            f"- 情报通道：Tushare 市场情报 `{payload.get('market_hits_count', 0)}` 条，RSS/topic search `{payload.get('search_hits_count', 0)}` 条，单标的情报 `{payload.get('stock_hits_count', 0)}` 条，机构调研 `{payload.get('structured_hits_count', 0)}` 条。",
            f"- 说明：{str(payload.get('note') or '').strip() or '—'}",
            f"- 边界：{str(payload.get('disclosure') or '').strip() or '—'}",
        ]
    )
    return "\n".join(lines).strip() + "\n"


def main() -> None:
    args = build_parser().parse_args()
    config = load_config(args.config or None)
    query = " ".join(args.query).strip()
    disable = getattr(logger, "disable", None)
    enable = getattr(logger, "enable", None)
    disabled_modules = ("src.collectors.base", "src.collectors.news")
    if callable(disable):
        for module_name in disabled_modules:
            disable(module_name)
    with redirect_stderr(io.StringIO()):
        try:
            payload = collect_intel_payload(
                query,
                config=config,
                explicit_symbol=args.symbol,
                limit=args.limit,
                recent_days=args.recent_days,
                structured_only=bool(args.structured_only),
            )
        finally:
            if callable(enable):
                for module_name in disabled_modules:
                    enable(module_name)
    print(_render_intel_markdown(payload))


if __name__ == "__main__":
    main()
