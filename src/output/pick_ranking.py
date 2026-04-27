"""Shared ranking helpers for pick-style client reports."""

from __future__ import annotations

import re
from typing import Any, List, Mapping, Optional, Sequence

from src.storage.strategy import StrategyRepository


_STRATEGY_CONFIDENCE_CACHE: dict[str, dict[str, Any]] = {}


def _strategy_background_confidence(analysis: Mapping[str, Any]) -> dict[str, Any]:
    embedded = dict(analysis.get("strategy_background_confidence") or {})
    if embedded:
        return embedded
    symbol = str(analysis.get("symbol", "") or "").strip()
    if not symbol:
        return {}
    if symbol not in _STRATEGY_CONFIDENCE_CACHE:
        try:
            _STRATEGY_CONFIDENCE_CACHE[symbol] = dict(StrategyRepository().summarize_background_confidence(symbol) or {})
        except Exception:
            _STRATEGY_CONFIDENCE_CACHE[symbol] = {}
    return dict(_STRATEGY_CONFIDENCE_CACHE[symbol] or {})


def score_dimension(analysis: Mapping[str, Any], dimension: str) -> int:
    value = analysis.get("dimensions", {}).get(dimension, {}).get("score")
    try:
        return int(value or 0)
    except Exception:
        return 0


def average_dimension_score(analysis: Mapping[str, Any]) -> float:
    values: list[float] = []
    for dimension in dict(analysis.get("dimensions") or {}).values():
        score = dict(dimension or {}).get("score")
        try:
            value = float(score)
        except Exception:
            continue
        if value >= 0:
            values.append(value)
    if not values:
        return 0.0
    return sum(values) / len(values)


def score_band(score: float, *, width: float = 5.0) -> int:
    try:
        value = float(score)
    except Exception:
        return 0
    width = max(float(width or 0.0), 1.0)
    return int(value // width)


def _etf_requires_observe_bucket(analysis: Mapping[str, Any]) -> bool:
    asset_type = str(analysis.get("asset_type", "") or "").strip()
    if asset_type not in {"cn_etf", "cn_fund", "cn_index"}:
        return False
    catalyst_dimension = dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {})
    coverage = dict(catalyst_dimension.get("coverage") or {})
    has_direct_confirmation = bool(
        coverage.get("structured_event")
        or coverage.get("effective_structured_event")
        or coverage.get("forward_event")
        or coverage.get("high_confidence_company_news")
        or int(coverage.get("direct_news_count") or 0) > 0
        or int(coverage.get("fresh_direct_news_count") or 0) > 0
    )
    has_theme_confirmation = bool(coverage.get("directional_catalyst_hit")) or int(coverage.get("theme_news_count") or 0) > 0
    if has_direct_confirmation or has_theme_confirmation:
        return False
    technical = score_dimension(analysis, "technical")
    catalyst = score_dimension(analysis, "catalyst")
    risk = score_dimension(analysis, "risk")
    diagnosis = str(coverage.get("diagnosis", "") or "").strip()
    return diagnosis in {"proxy_degraded", "theme_only_live", "stale_live_only"} and catalyst < 20 and technical < 50 and risk < 45


def recommendation_bucket(
    analysis: Mapping[str, Any],
    watch_symbols: Optional[set[str]] = None,
) -> str:
    if bool(analysis.get("excluded")):
        return "观察为主"
    if _etf_requires_observe_bucket(analysis):
        return "观察为主"
    rating_rank = int(analysis.get("rating", {}).get("rank", 0) or 0)
    technical = score_dimension(analysis, "technical")
    fundamental = score_dimension(analysis, "fundamental")
    catalyst = score_dimension(analysis, "catalyst")
    relative = score_dimension(analysis, "relative_strength")
    risk = score_dimension(analysis, "risk")

    support_dims = sum(score >= 60 for score in (technical, fundamental, catalyst, relative, risk))
    positive_dims = sum(score >= 60 for score in (fundamental, catalyst, relative, risk))
    elite_positive = max(fundamental, catalyst, relative, risk) >= 80

    def qualified_watch() -> bool:
        return (
            positive_dims >= 2
            or (elite_positive and technical >= 35)
            or (fundamental >= 75 and catalyst >= 30 and technical >= 30)
            or (catalyst >= 60 and relative >= 45 and technical >= 25)
            or (relative >= 70 and technical >= 45)
            or support_dims >= 3
        )

    if rating_rank >= 3:
        return "正式推荐"
    if rating_rank >= 2:
        return "看好但暂不推荐"
    if watch_symbols and str(analysis.get("symbol", "")).strip() in watch_symbols and qualified_watch():
        if strategy_blocks_soft_watch_upgrade(analysis):
            return "观察为主"
        return "看好但暂不推荐"
    if qualified_watch():
        if strategy_blocks_soft_watch_upgrade(analysis):
            return "观察为主"
        return "看好但暂不推荐"
    return "观察为主"


def bucket_priority(bucket: str) -> int:
    return {
        "正式推荐": 0,
        "看好但暂不推荐": 1,
        "观察为主": 2,
    }.get(bucket, 9)


def portfolio_overlap_priority(analysis: Mapping[str, Any]) -> int:
    summary = dict(analysis.get("portfolio_overlap_summary") or {})
    if not summary:
        return 1

    overlap_label = str(summary.get("overlap_label", "")).strip()
    style_conflict = str(summary.get("style_conflict_label", "")).strip()

    try:
        same_symbol_weight = float(summary.get("same_symbol_weight", 0.0) or 0.0)
    except Exception:
        same_symbol_weight = 0.0
    try:
        same_sector_weight = float(summary.get("same_sector_weight", 0.0) or 0.0)
    except Exception:
        same_sector_weight = 0.0
    try:
        same_region_weight = float(summary.get("same_region_weight", 0.0) or 0.0)
    except Exception:
        same_region_weight = 0.0

    same_symbol = same_symbol_weight > 0
    same_sector = same_sector_weight >= 0.20 or overlap_label in {"同一行业主线加码", "主题/行业重复较高"}
    same_region = same_region_weight >= 0.35 or overlap_label in {"地区暴露偏重", "地区重复度偏高"}

    if same_symbol or style_conflict == "同风格重复较高":
        return 3
    if same_sector or same_region or style_conflict == "同风格延伸":
        return 2
    if style_conflict in {"风格补位", "可做风格补位"}:
        return 0
    return 1


def portfolio_overlap_bonus(analysis: Mapping[str, Any]) -> int:
    return {
        0: 3,
        1: 2,
        2: 1,
        3: 0,
    }.get(portfolio_overlap_priority(analysis), 2)


def strategy_confidence_priority(analysis: Mapping[str, Any]) -> int:
    status = strategy_confidence_status(analysis)
    return {
        "stable": 0,
        "watch": 1,
        "": 1,
        "degraded": 2,
    }.get(status, 1)


def strategy_confidence_status(analysis: Mapping[str, Any]) -> str:
    return str(_strategy_background_confidence(analysis).get("status", "")).strip()


def strategy_blocks_soft_watch_upgrade(analysis: Mapping[str, Any]) -> bool:
    return strategy_confidence_status(analysis) in {"watch", "degraded"}


def _client_safe_line(text: Any) -> str:
    line = str(text).strip()
    if not line:
        return ""
    replacements = (
        (r"开盘\s*30\s*分钟", "早段"),
        (r"开盘后先观察\s*\d+\s*分钟", "先观察早段延续性"),
        (r"明天开盘前", "明早"),
        (r"盘中", "交易时段"),
        (r"隔日涨跌", "短期涨跌"),
        (r"只按隔夜消息", "只按单条消息"),
        (r"纯隔夜交易", "纯超短交易"),
        (r"隔夜交易", "超短交易"),
    )
    for pattern, repl in replacements:
        line = re.sub(pattern, repl, line)
    return line


def analysis_is_actionable(
    analysis: Mapping[str, Any],
    watch_symbols: Optional[set[str]] = None,
) -> bool:
    bucket = recommendation_bucket(analysis, watch_symbols)
    if bucket == "正式推荐":
        return True
    if bucket != "看好但暂不推荐":
        return False

    action = dict(analysis.get("action") or {})
    direction = _client_safe_line(action.get("direction", ""))
    position = _client_safe_line(action.get("position", ""))
    entry = _client_safe_line(action.get("entry", ""))
    combined = " / ".join(part for part in (direction, position, entry) if part)
    non_action_markers = (
        "暂不出手",
        "仅观察仓",
        "先按观察仓",
        "先观察",
        "观察为主",
        "回避",
        "等待更好窗口",
        "触发前先别急着",
    )
    if any(marker in combined for marker in non_action_markers):
        return False
    if "观望" in direction and not any(token in combined for token in ("试仓", "建仓", "小仓", "%", "分批")):
        return False
    return True


def rank_market_items(
    items: Sequence[Mapping[str, Any]],
    watch_symbols: set[str],
) -> List[Mapping[str, Any]]:
    return sorted(
        items,
        key=lambda item: (
            bucket_priority(recommendation_bucket(item, watch_symbols)),
            -int(item.get("rating", {}).get("rank", 0) or 0),
            strategy_confidence_priority(item),
            -score_band(average_dimension_score(item)),
            portfolio_overlap_priority(item),
            -average_dimension_score(item),
            -score_dimension(item, "relative_strength"),
            -score_dimension(item, "fundamental"),
        ),
    )
