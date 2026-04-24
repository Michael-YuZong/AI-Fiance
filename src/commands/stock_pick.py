"""Stock pick command — scan stock universe and surface top individual stock picks."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from datetime import datetime
import json
import re
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from src.commands.final_runner import finalize_client_markdown, internal_sidecar_path
from src.commands.intel import collect_intel_news_report, collect_market_aware_intel_news_report
from src.commands.pick_history import enrich_pick_payload_with_score_history, summarize_pick_coverage
from src.commands.pick_visuals import attach_visuals_to_analyses
from src.commands.report_guard import ensure_report_task_registered, exported_bundle_lines
from src.output import ClientReportRenderer, OpportunityReportRenderer
from src.output.catalyst_web_review import (
    attach_catalyst_web_review_to_analysis,
    build_catalyst_web_review_packet,
    load_catalyst_web_review,
    render_catalyst_web_review_prompt,
    render_catalyst_web_review_scaffold,
)
from src.output.editor_payload import (
    build_stock_pick_editor_packet,
    render_editor_homepage,
    render_financial_editor_prompt,
    summarize_theme_playbook_contract,
    summarize_what_changed_contract,
)
from src.output.event_digest import summarize_event_digest_contract
from src.output.client_report import _upsert_visual_section, render_event_digest_section, render_what_changed_section
from src.output.pick_ranking import average_dimension_score, portfolio_overlap_bonus, rank_market_items, recommendation_bucket, score_band
from src.processors.factor_meta import summarize_factor_contracts_from_analyses
from src.processors.portfolio_actions import attach_portfolio_overlap_summaries
from src.processors.opportunity_engine import _client_safe_issue, analyze_opportunity, build_market_context, discover_stock_opportunities, summarize_proxy_contracts_from_analyses
from src.utils.config import PROJECT_ROOT, load_config
from src.utils.logger import setup_logger
from src.utils.market import close_yfinance_runtime_caches

SNAPSHOT_PATH = PROJECT_ROOT / "data" / "stock_pick_score_history.json"
FINAL_DIR = PROJECT_ROOT / "reports" / "stock_picks" / "final"
INTERNAL_DIR = PROJECT_ROOT / "reports" / "stock_picks" / "internal"
MODEL_VERSION = "stock-pick-2026-03-14-candlestick-v8"
MODEL_CHANGELOG = [
    "A 股估值口径统一为 `PE_TTM`；动态 PE 不再混入滚动 PE。",
    "个股负面事件窗口扩展为 `30` 日衰减，并补了英文监管/稀释关键词。",
    "同一天的报告默认锁定首个可用输出为 `当日基准版`；后续重跑统一和基准版对比。",
    "风险维度的回撤恢复改为看 `近一年高点后的修复速度/修复比例`，不再把长期未创新高统一打成 `999 日`。",
    "DMI/ADX 改为 `Wilder smoothing` 口径，不再用简单滚动均值；这会影响趋势强度分和技术面摘要。",
    "RSI 改为 Wilder 初始均值口径，KDJ 改为以 `50` 为种子递推，避免和主流行情软件出现系统性偏差。",
    "图表层不再重复自算技术指标，统一复用 `TechnicalAnalyzer` 输出，避免图表和报告口径分叉。",
    "技术面里的 `量比` 文案改为 `量能比`，明确表示这里使用的是日成交量相对 5 日均量。",
    "技术面新增 `量价/动量背离` 因子，按最近两组确认摆点检查 RSI / MACD / OBV 与价格是否出现顶/底背离。",
    "K 线形态从“单根 K”升级到“最近 1-3 根组合形态”，会识别吞没、星形、三兵三鸦等常见反转/延续信号，并结合前序 5 日趋势过滤误报。",
    "催化面核心信号优先展示个股直连标题，减少所有股票都显示同一组市场新闻的问题。",
    "HK/US 个股前瞻事件改为优先读取公司级财报日历；未来 `14` 日财报日会进入催化和风险窗口。",
    "交易参数增加硬校验，默认满足 `止损价 < 当前价 < 目标价`，避免把阻力位误标成止损。",
    "HK/US 个股若未命中公司直连新闻，政策/龙头/海外映射催化不再做正向加分，避免把市场级新闻误记成个股催化。",
    "英文股票名不再使用两字符前缀做模糊匹配，避免 `Meta -> Me` 这类误命中。",
    "美股短英文 ticker 改为按单词边界匹配，避免 `SNOW -> snowfall` 这类误命中。",
]
FAST_STRUCTURED_STOCK_INTELLIGENCE_APIS = [
    "forecast",
    "express",
    "dividend",
    "stk_surv",
    "irm_qa_sh",
    "irm_qa_sz",
]
EXPECTED_CHART_THEMES = ("terminal", "abyss-gold", "institutional", "clinical", "erdtree", "neo-brutal")


def _visuals_have_theme_variants(visuals: Mapping[str, Any] | None) -> bool:
    if not isinstance(visuals, Mapping):
        return False
    for key in ("dashboard", "windows", "indicators"):
        raw_path = str(visuals.get(key, "")).strip()
        if not raw_path:
            continue
        path = Path(raw_path)
        if not path.is_absolute():
            path = (PROJECT_ROOT / path).resolve()
        suffix = path.suffix
        base_stem = path.stem.split(".theme-", 1)[0]
        for theme_name in EXPECTED_CHART_THEMES:
            variant_path = path.with_name(f"{base_stem}.theme-{theme_name}{suffix}")
            if not variant_path.exists():
                return False
    return True


def _featured_visual_clusters(payload: Mapping[str, Any]) -> Dict[tuple[str, str, str], list[Dict[str, Any]]]:
    top = list(dict(payload or {}).get("top") or [])
    if not top:
        return {}
    clustered: Dict[tuple[str, str, str], list[Dict[str, Any]]] = {}
    for field in ("top", "watch_positive", "coverage_analyses"):
        for item in payload.get(field) or []:
            if not isinstance(item, dict):
                continue
            key = _visual_key(item)
            if not key[1]:
                continue
            clustered.setdefault(key, []).append(item)
    watch_symbols = {
        str(item.get("symbol", ""))
        for item in (payload.get("watch_positive") or [])
        if str(item.get("symbol", "")).strip()
    }
    grouped: Dict[str, list[Mapping[str, Any]]] = {"A股": [], "港股": [], "美股": []}
    for item in top:
        label = {"cn_stock": "A股", "hk": "港股", "us": "美股"}.get(str(item.get("asset_type", "")), "")
        if label:
            grouped.setdefault(label, []).append(item)

    featured_keys: list[tuple[str, str, str]] = []
    for market_name in ("A股", "港股", "美股"):
        items = grouped.get(market_name) or []
        if not items:
            continue
        ranked = rank_market_items(items, watch_symbols)
        featured_keys.extend(_visual_key(item) for item in ranked[:3] if _visual_key(item)[1])
    return {
        key: cluster
        for key in dict.fromkeys(featured_keys)
        if (cluster := clustered.get(key) or [])
    }


def _rehydrate_featured_visual_theme_variants(
    payload: Dict[str, Any],
    config: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    if not config:
        return payload
    for cluster in _featured_visual_clusters(payload).values():
        source = max(cluster, key=_visual_source_score)
        if _visuals_have_theme_variants(source.get("visuals")):
            continue
        symbol = str(source.get("symbol", "")).strip()
        asset_type = str(source.get("asset_type", "")).strip()
        if not symbol or not asset_type:
            continue
        try:
            refreshed = analyze_opportunity(
                symbol,
                asset_type,
                config,
                metadata_override=dict(source.get("metadata") or {}),
            )
        except Exception:
            continue
        stamp = str(source.get("generated_at", "")).strip()
        if stamp:
            refreshed["generated_at"] = stamp
        attach_visuals_to_analyses([refreshed], render_theme_variants=True)
        visuals = dict(refreshed.get("visuals") or {})
        if not _visuals_have_theme_variants(visuals):
            continue
        for sibling in cluster:
            sibling["visuals"] = dict(visuals)
    return payload


def _shared_intel_news_report(
    config: Mapping[str, Any],
    *,
    query: str,
    explicit_symbol: str = "",
    baseline_report: Mapping[str, Any] | None = None,
    limit: int = 8,
    recent_days: int = 7,
    note_prefix: str = "",
    structured_only: Optional[bool] = None,
) -> Dict[str, Any]:
    try:
        report = collect_market_aware_intel_news_report(
            query,
            config=config,
            explicit_symbol=explicit_symbol,
            baseline_report=baseline_report,
            limit=limit,
            recent_days=recent_days,
            structured_only=(
                bool(structured_only)
                if structured_only is not None
                else not bool(dict(config or {}).get("news_topic_search_enabled", True))
            ),
            note_prefix=note_prefix,
            collect_fn=collect_intel_news_report,
        )
    except Exception:
        return {}
    if not list(dict(report).get("items") or []):
        return {}
    return dict(report)


def _attach_shared_intel_news_report(
    context: Mapping[str, Any],
    config: Mapping[str, Any],
    *,
    query: str,
    explicit_symbol: str = "",
    note_prefix: str = "",
    limit: int = 8,
    recent_days: int = 7,
    structured_only: Optional[bool] = None,
) -> Dict[str, Any]:
    merged = dict(context or {})
    existing_report = dict(merged.get("news_report") or {})
    shared_report = _shared_intel_news_report(
        config,
        query=query,
        explicit_symbol=explicit_symbol,
        baseline_report=existing_report,
        limit=limit,
        recent_days=recent_days,
        note_prefix=note_prefix,
        structured_only=structured_only,
    )
    if not shared_report:
        return merged

    existing_items = list(existing_report.get("items") or [])
    shared_items = list(shared_report.get("items") or [])
    if not existing_items or len(shared_items) >= len(existing_items):
        merged["news_report"] = shared_report
        merged["intel_news_report"] = shared_report
    return merged


def _market_intel_query(market: str, sector_filter: str) -> str:
    market_label = {"cn": "A股", "hk": "港股", "us": "美股", "all": "全市场"}.get(str(market).strip(), str(market).strip() or "A股")
    terms = [market_label]
    if str(sector_filter or "").strip():
        terms.append(str(sector_filter).strip())
    terms.extend(["主线", "情报"])
    return " ".join(dict.fromkeys(term for term in terms if term))


def _finalist_intel_query(finalists: Sequence[Mapping[str, Any]]) -> str:
    terms: list[str] = []
    for item in list(finalists or [])[:3]:
        for value in (
            str(item.get("name", "")).strip(),
            str(item.get("symbol", "")).strip(),
            str(item.get("sector", "")).strip(),
        ):
            if value and value not in terms:
                terms.append(value)
    return " ".join(terms) if terms else "A股 股票 情报"


def _run_timed_stock_analysis(
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    *,
    context: Mapping[str, Any] | None = None,
    metadata_override: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    loader = lambda: analyze_opportunity(  # noqa: E731
        symbol,
        asset_type,
        config,
        context=context,
        metadata_override=metadata_override,
    )
    return loader()


def _client_final_runtime_overrides(
    config: Mapping[str, Any],
    *,
    client_final: bool,
    explicit_config_path: str = "",
) -> tuple[Dict[str, Any], list[str]]:
    if not client_final:
        return deepcopy(dict(config or {})), []

    effective = deepcopy(dict(config or {}))
    notes: list[str] = []

    market_context = dict(effective.get("market_context") or {})
    proxy_changed = False
    if not bool(market_context.get("skip_global_proxy")):
        market_context["skip_global_proxy"] = True
        proxy_changed = True
    if not bool(market_context.get("skip_market_monitor")):
        market_context["skip_market_monitor"] = True
        proxy_changed = True
    if proxy_changed:
        effective["market_context"] = market_context
        notes.append("为保证个股 `client-final` 可交付，本轮自动跳过跨市场代理与 market monitor；板块驱动保留给个股主链做行业/主题判断。")

    if not bool(effective.get("news_topic_search_enabled", False)):
        effective["news_topic_search_enabled"] = True
        notes.append("本轮 `client-final` 保留受控个股/主题情报回填，避免强方向只剩技术面而缺催化确认。")
    current_news_feeds = str(effective.get("news_feeds_file", "") or "").strip()
    if current_news_feeds != "config/news_feeds.briefing_light.yaml":
        effective["news_feeds_file"] = "config/news_feeds.briefing_light.yaml"
        notes.append("本轮 `client-final` 已自动切到轻量但非空的新闻源配置，保留少量可点击情报，不再把个股催化链整段关空。")

    opportunity = dict(effective.get("opportunity") or {})
    if int(opportunity.get("analysis_workers", 4) or 4) > 3:
        opportunity["analysis_workers"] = 3
        notes.append("本轮 `client-final` 已自动收窄个股分析并发，优先保证正式稿稳定落盘。")
    current_candidates = int(opportunity.get("stock_max_scan_candidates", 60) or 60)
    normalized_candidates = 24
    if current_candidates != normalized_candidates:
        opportunity["stock_max_scan_candidates"] = normalized_candidates
        notes.append("本轮 `client-final` 已重设并收窄个股候选池到 `18` 只，避免 fast 配置把正式推荐池收窄得过头。")
    if opportunity:
        effective["opportunity"] = opportunity

    if list(effective.get("structured_stock_intelligence_apis") or []) != FAST_STRUCTURED_STOCK_INTELLIGENCE_APIS:
        effective["structured_stock_intelligence_apis"] = list(FAST_STRUCTURED_STOCK_INTELLIGENCE_APIS)
        notes.append("本轮 `client-final` 已自动聚焦个股最关键的结构化情报源（业绩预告/快报/分红/机构调研/互动平台问答），避免结构化情报慢链拖住正式稿。")
    if str(effective.get("stock_news_runtime_mode", "") or "").strip().lower() != "finalist":
        effective["stock_news_runtime_mode"] = "finalist"
        notes.append("本轮 `client-final` 的正式候选补强已切到 finalist 模式：先吃结构化/官方直连，再做极少量主题回填，不再对每只票跑一轮低质全量扩搜。")

    if int(effective.get("stock_news_limit", 10) or 10) > 8:
        effective["stock_news_limit"] = 8
        notes.append("本轮 `client-final` 已自动收紧单票情报条数上限，但会保留足够多的一手催化与确认线索。")
    effective["stock_news_finalist_official_query_cap"] = 2
    effective["stock_news_finalist_search_query_cap"] = 1
    effective["stock_news_finalist_search_recent_days"] = 21

    effective["skip_analysis_proxy_signals_runtime"] = True
    effective["skip_signal_confidence_runtime"] = True
    effective["stock_pool_skip_industry_lookup_runtime"] = True
    effective["stock_pool_prefer_cached_realtime_runtime"] = True
    notes.append("本轮 `client-final` 已自动跳过情绪代理、历史信号置信度和逐票行业补查慢链，优先保证正式稿稳定落盘。")
    notes.append("本轮 `client-final` 的候选池会优先复用本地 A 股快照缓存，不再为 live 实时快照白等长超时。")
    notes.append("本轮 `client-final` 不再给整段个股分析套线程超时；正式稿稳定性改由底层 fetcher timeout 和快照缓存兜住，避免把观察池误裁空。")

    if explicit_config_path.strip():
        notes.append("显式配置文件仍会继续叠加 `client-final` 的正式稿稳定性 guard，不会因为自定义配置而回退到慢链。")

    return effective, notes


def _preview_runtime_overrides(
    config: Mapping[str, Any],
    *,
    explicit_config_path: str = "",
) -> tuple[Dict[str, Any], list[str]]:
    if explicit_config_path.strip():
        return deepcopy(dict(config or {})), []

    effective = deepcopy(dict(config or {}))
    notes: list[str] = []

    market_context = dict(effective.get("market_context") or {})
    changed = False
    if not bool(market_context.get("skip_global_proxy")):
        market_context["skip_global_proxy"] = True
        changed = True
    if not bool(market_context.get("skip_market_monitor")):
        market_context["skip_market_monitor"] = True
        changed = True
    if changed:
        effective["market_context"] = market_context
        notes.append("默认预览已自动跳过跨市场代理与 market monitor；板块驱动保留给个股主链做行业/主题行情判断。")

    opportunity = dict(effective.get("opportunity") or {})
    if int(opportunity.get("analysis_workers", 4) or 4) > 3:
        opportunity["analysis_workers"] = 3
        notes.append("默认预览已自动收窄分析并发，减少 stock_pick 首次运行时长。")
    if int(opportunity.get("stock_max_scan_candidates", 60) or 60) > 18:
        opportunity["stock_max_scan_candidates"] = 18
        notes.append("默认预览已自动收窄候选池，但会保留足够多的板块强势样本，避免热方向只剩 1-2 只票。")
    if opportunity:
        effective["opportunity"] = opportunity

    if not bool(effective.get("news_topic_search_enabled", False)):
        effective["news_topic_search_enabled"] = True
        notes.append("默认预览保留受控主题情报扩搜，避免热点方向只剩技术面而找不到催化确认。")

    if list(effective.get("structured_stock_intelligence_apis") or []) != FAST_STRUCTURED_STOCK_INTELLIGENCE_APIS:
        effective["structured_stock_intelligence_apis"] = list(FAST_STRUCTURED_STOCK_INTELLIGENCE_APIS)
        notes.append("默认预览已自动聚焦业绩预告/快报/分红/机构调研/互动平台问答五类结构化情报，减少 stock_pick 慢链。")
    if str(effective.get("stock_news_runtime_mode", "") or "").strip().lower() != "focused":
        effective["stock_news_runtime_mode"] = "focused"
        notes.append("默认预览已切到逐票公司情报 focused 模式：先看结构化/官方线索，只做少量搜索回填，避免每只票都跑一轮低质扩搜。")

    if int(effective.get("stock_news_limit", 10) or 10) > 8:
        effective["stock_news_limit"] = 8
        notes.append("默认预览已自动收紧单票情报条数上限，但会保留足够多的直连催化和主题确认线索。")

    effective["skip_catalyst_dynamic_search_runtime"] = True
    effective["stock_news_official_query_cap"] = 2
    effective["stock_news_search_query_cap"] = 1
    effective["stock_news_search_recent_days"] = 14
    notes.append("默认预览会把主题动态扩搜留给入围样本，预筛阶段只保留高价值官方/结构化催化。")

    effective["skip_analysis_proxy_signals_runtime"] = True
    effective["skip_signal_confidence_runtime"] = True
    effective["stock_pool_skip_industry_lookup_runtime"] = True
    effective["stock_pool_prefer_cached_realtime_runtime"] = True
    notes.append("默认预览已自动跳过情绪代理、历史信号置信度和逐票行业补查慢链，优先保证首屏筛选速度。")
    notes.append("默认预览会优先复用本地 A 股快照缓存，避免 stock_pool 为 live 实时快照白等长超时。")
    notes.append("默认预览不再给整段个股分析套线程超时；首屏稳定性改由底层 fetcher timeout 和快照缓存兜住。")

    return effective, notes


def _client_final_discovery_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    effective = deepcopy(dict(config or {}))
    effective["skip_index_topic_bundle_runtime"] = True
    effective["skip_catalyst_dynamic_search_runtime"] = True
    effective["stock_news_runtime_mode"] = "focused"
    effective["stock_news_official_query_cap"] = 2
    effective["stock_news_search_query_cap"] = 1
    effective["stock_news_search_recent_days"] = 14
    return effective


def _stock_pick_finalist_reanalysis_config(config: Mapping[str, Any]) -> Dict[str, Any]:
    effective = deepcopy(dict(config or {}))
    effective["stock_news_runtime_mode"] = "finalist"
    effective["stock_news_finalist_official_query_cap"] = 2
    effective["stock_news_finalist_search_query_cap"] = 1
    effective["stock_news_finalist_search_recent_days"] = 21
    effective["skip_catalyst_dynamic_search_runtime"] = False
    return effective


def _scope_key(market: str, sector_filter: str) -> str:
    return f"{market}:{sector_filter or '*'}"

def _coverage_summary(analyses: list[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = list(analyses or [])
    grouped: Dict[str, list[Mapping[str, Any]]] = {"A股": [], "港股": [], "美股": []}
    for item in rows:
        label = {"cn_stock": "A股", "hk": "港股", "us": "美股"}.get(str(item.get("asset_type", "")), "")
        if label:
            grouped.setdefault(label, []).append(item)

    by_market: Dict[str, Dict[str, Any]] = {}
    lines: list[str] = []
    for market in ("A股", "港股", "美股"):
        market_rows = grouped.get(market) or []
        if not market_rows:
            continue
        summary = summarize_pick_coverage(market_rows)
        total = int(summary.get("total", 0) or 0)
        structured = int(round(float(summary.get("structured_rate", 0.0) or 0.0) * total))
        direct = int(round(float(summary.get("direct_news_rate", 0.0) or 0.0) * total))
        by_market[market] = {
            "total": total,
            "news_mode": str(summary.get("news_mode", "")),
            "degraded": bool(summary.get("degraded")),
            "structured_rate": float(summary.get("structured_rate", 0.0) or 0.0),
            "direct_rate": float(summary.get("direct_news_rate", 0.0) or 0.0),
        }
        lines.append(
            f"{market} 结构化事件覆盖 {float(summary.get('structured_rate', 0.0) or 0.0) * 100:.0f}%（{structured}/{total}）"
            f" / 高置信公司新闻覆盖 {float(summary.get('direct_news_rate', 0.0) or 0.0) * 100:.0f}%（{direct}/{total}）"
        )
    overall = summarize_pick_coverage(rows)
    return {
        "by_market": by_market,
        "lines": lines,
        "note": str(overall.get("note", "当前没有可统计的候选样本。")),
        "total": int(overall.get("total", 0) or 0),
        "news_mode": str(overall.get("news_mode", "unknown")),
        "degraded": bool(overall.get("degraded")),
    }


def _analysis_key(item: Mapping[str, Any]) -> tuple[str, str]:
    return (str(item.get("asset_type", "")).strip(), str(item.get("symbol", "")).strip())


def _stock_pick_finalist_candidates(payload: Mapping[str, Any], top_n: int) -> list[Mapping[str, Any]]:
    coverage_rows = list(payload.get("coverage_analyses") or payload.get("top") or [])
    if not coverage_rows:
        return []
    finalist_limit = min(len(coverage_rows), max(5, min(6, max(1, top_n))))
    watch_symbols = {str(item).strip() for item in list(payload.get("watch_symbols") or []) if str(item).strip()}
    finalists: list[Mapping[str, Any]] = []
    seen: set[tuple[str, str]] = set()
    for bucket in (
        list(payload.get("top") or [])[:finalist_limit],
        list(payload.get("watch_positive") or [])[:finalist_limit],
        coverage_rows[:finalist_limit],
    ):
        for item in bucket:
            key = _analysis_key(item)
            if not key[1] or key in seen:
                continue
            finalists.append(item)
            seen.add(key)
    finalists.sort(
        key=lambda item: (
            {"正式推荐": 0, "看好但暂不推荐": 1, "观察为主": 2}.get(recommendation_bucket(item, watch_symbols), 9),
            -int(dict(item.get("rating") or {}).get("rank", 0) or 0),
            -score_band(average_dimension_score(item)),
            -average_dimension_score(item),
        )
    )
    return finalists[:finalist_limit]


def _analysis_metadata_override(item: Mapping[str, Any]) -> Dict[str, Any]:
    metadata = dict(item.get("metadata") or {})
    override: Dict[str, Any] = dict(metadata)
    name = str(item.get("name", "")).strip()
    if name:
        override.setdefault("name", name)
    sector = str(override.get("sector") or item.get("sector", "")).strip()
    if sector:
        override["sector"] = sector
    chain_nodes = [str(node).strip() for node in list(override.get("chain_nodes") or []) if str(node).strip()]
    if chain_nodes:
        override["chain_nodes"] = chain_nodes
    region = str(override.get("region", "")).strip()
    if region:
        override["region"] = region
    if "in_watchlist" in override:
        override["in_watchlist"] = bool(override.get("in_watchlist"))
    return override


def _reenrich_stock_pick_finalists(
    payload: Dict[str, Any],
    config: Mapping[str, Any],
    *,
    top_n: int,
    context: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    finalist_config = _stock_pick_finalist_reanalysis_config(config)
    finalists = _stock_pick_finalist_candidates(payload, top_n)
    if not finalists:
        return payload

    if context is None:
        relevant_asset_types = list(
            dict.fromkeys(
                [
                    str(item.get("asset_type", "")).strip()
                    for item in finalists
                    if str(item.get("asset_type", "")).strip()
                ]
            )
        ) or ["cn_stock", "cn_etf", "futures"]
        base_context = build_market_context(config, relevant_asset_types=relevant_asset_types)
    else:
        base_context = dict(context)
    base_context = _attach_shared_intel_news_report(
        base_context,
        config,
        query=_finalist_intel_query(finalists),
        explicit_symbol=str(finalists[0].get("symbol", "")).strip() if finalists else "",
        note_prefix="stock_pick finalist intel: ",
        structured_only=True,
    )

    opportunity_cfg = dict(dict(config).get("opportunity") or {})
    workers = max(1, min(int(opportunity_cfg.get("analysis_workers", 2) or 2), len(finalists), 3))
    refreshed: Dict[tuple[str, str], Dict[str, Any]] = {}
    blind_spots = [str(item).strip() for item in list(payload.get("blind_spots") or []) if str(item).strip()]

    def _context_for_item() -> Dict[str, Any]:
        return {**base_context, "runtime_caches": {}}

    if workers > 1 and len(finalists) > 1:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            future_map = {
                executor.submit(
                    _run_timed_stock_analysis,
                    str(item.get("symbol", "")).strip(),
                    str(item.get("asset_type", "")).strip(),
                    finalist_config,
                    context=_context_for_item(),
                    metadata_override=_analysis_metadata_override(item),
                ): item
                for item in finalists
                if str(item.get("symbol", "")).strip() and str(item.get("asset_type", "")).strip()
            }
            for future in as_completed(future_map):
                item = future_map[future]
                try:
                    refreshed[_analysis_key(item)] = future.result()
                except Exception as exc:
                    blind_spots.append(_client_safe_issue(f"{item.get('symbol', '')} ({item.get('name', '')}) 正式候选补强失败", exc))
    else:
        for item in finalists:
            symbol = str(item.get("symbol", "")).strip()
            asset_type = str(item.get("asset_type", "")).strip()
            if not symbol or not asset_type:
                continue
            try:
                refreshed[_analysis_key(item)] = _run_timed_stock_analysis(
                    symbol,
                    asset_type,
                    finalist_config,
                    context=_context_for_item(),
                    metadata_override=_analysis_metadata_override(item),
                )
            except Exception as exc:
                blind_spots.append(_client_safe_issue(f"{symbol} ({item.get('name', '')}) 正式候选补强失败", exc))

    if not refreshed:
        payload["blind_spots"] = list(dict.fromkeys(blind_spots))
        return payload

    for field in ("top", "watch_positive", "coverage_analyses"):
        rows = list(payload.get(field) or [])
        if not rows:
            continue
        payload[field] = [refreshed.get(_analysis_key(item), item) for item in rows]
    payload["blind_spots"] = list(dict.fromkeys(blind_spots))
    return payload


def _rank_key(item: Mapping[str, Any]) -> tuple[float, float, float, float, float, float]:
    dimensions = dict(item.get("dimensions") or {})
    total_score = float(
        sum(float(dict(dimension).get("score") or 0) for dimension in dimensions.values())
    )
    average_score = average_dimension_score(item)
    return (
        float(int(item.get("rating", {}).get("rank", 0) or 0)),
        float(score_band(average_score)),
        float(portfolio_overlap_bonus(item)),
        average_score,
        float(dict(dimensions.get("relative_strength") or {}).get("score") or 0),
        float(dict(dimensions.get("fundamental") or {}).get("score") or 0),
    )


def _watch_positive_candidates(analyses: list[Mapping[str, Any]]) -> list[Mapping[str, Any]]:
    return sorted(
        [
            analysis
            for analysis in analyses
            if int(dict(analysis.get("rating") or {}).get("rank", 0) or 0) < 3
            and (
                (dict(dict(analysis.get("dimensions") or {}).get("fundamental") or {}).get("score") or 0) >= 60
                or (dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {}).get("score") or 0) >= 50
                or (dict(dict(analysis.get("dimensions") or {}).get("relative_strength") or {}).get("score") or 0) >= 70
                or (dict(dict(analysis.get("dimensions") or {}).get("risk") or {}).get("score") or 0) >= 70
            )
        ],
        key=_rank_key,
        reverse=True,
    )[:6]


def _observe_fallback_top_candidates(
    analyses: Sequence[Mapping[str, Any]],
    *,
    limit: int,
) -> list[Mapping[str, Any]]:
    rows = list(analyses or [])
    if not rows:
        return []
    watch_rows = _watch_positive_candidates(rows)
    ranked_rows = watch_rows or sorted(rows, key=_rank_key, reverse=True)
    return list(ranked_rows[: max(int(limit or 0), 1)])


def _factor_contract_summary(analyses: list[Mapping[str, Any]]) -> Dict[str, Any]:
    return summarize_factor_contracts_from_analyses(list(analyses or []), sample_limit=16)


def _visual_key(item: Mapping[str, Any]) -> tuple[str, str, str]:
    return (
        str(item.get("asset_type", "")).strip(),
        str(item.get("symbol", "")).strip(),
        str(item.get("generated_at", "")).strip(),
    )


def _visual_source_score(item: Mapping[str, Any]) -> int:
    return (
        int(bool(item.get("visuals"))) * 100
        + int(item.get("history") is not None) * 10
        + int(bool(item.get("technical_raw"))) * 5
        + int(bool(item.get("dimensions"))) * 2
    )


def _attach_featured_visuals(
    payload: Dict[str, Any],
    *,
    render_theme_variants: bool = False,
) -> Dict[str, Any]:
    top = list(payload.get("top") or [])
    if not top:
        return payload
    for cluster in _featured_visual_clusters(payload).values():
        source = max(cluster, key=_visual_source_score)
        if render_theme_variants or not source.get("visuals") or not _visuals_have_theme_variants(source.get("visuals")):
            attach_visuals_to_analyses([source], render_theme_variants=render_theme_variants)
        visuals = dict(source.get("visuals") or {})
        if not visuals:
            continue
        for sibling in cluster:
            sibling["visuals"] = dict(visuals)
    return payload


def enrich_payload_with_score_history(
    payload: Dict[str, Any],
    market: str,
    sector_filter: str,
    snapshot_path: Path = SNAPSHOT_PATH,
) -> Dict[str, Any]:
    payload = enrich_pick_payload_with_score_history(
        payload,
        scope=_scope_key(market, sector_filter),
        snapshot_path=snapshot_path,
        model_version=MODEL_VERSION,
        model_changelog=MODEL_CHANGELOG,
        rank_key=_rank_key,
    )
    coverage_rows = list(payload.get("coverage_analyses", []) or payload.get("top", []) or [])
    payload["watch_positive"] = _watch_positive_candidates(coverage_rows)
    target_count = int(payload.get("requested_top_n", 0) or len(payload.get("top") or []) or 5)
    if not list(payload.get("top") or []) and coverage_rows:
        payload["top"] = _observe_fallback_top_candidates(coverage_rows, limit=target_count)
    payload["stock_pick_coverage"] = _coverage_summary(coverage_rows)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan stock universe and surface top individual stock picks.")
    parser.add_argument("--market", default="cn", choices=["cn", "hk", "us", "all"], help="Market scope: cn (A-share), hk, us, or all")
    parser.add_argument("--sector", default="", help="Sector filter, e.g. 科技 / 消费 / 医药")
    parser.add_argument("--top", type=int, default=20, help="Number of top picks to show")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")
    return parser


def _sector_suffix(sector_filter: str) -> str:
    text = str(sector_filter or "").strip()
    if not text:
        return ""
    text = re.sub(r"[\\/:*?\"<>|]+", "_", text)
    text = re.sub(r"\s+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return f"_{text}" if text else ""


def _internal_detail_stem(market: str, generated_at: str, sector_filter: str = "") -> str:
    return f"stock_picks_{market}{_sector_suffix(sector_filter)}_{generated_at[:10]}_internal_detail"


def _internal_merged_stem(generated_at: str, sector_filter: str = "") -> str:
    return f"stock_picks{_sector_suffix(sector_filter)}_{generated_at[:10]}_internal_detail"


def _final_stem(generated_at: str, sector_filter: str = "") -> str:
    return f"stock_picks{_sector_suffix(sector_filter)}_{generated_at[:10]}_final"


def _market_final_stem(market: str, generated_at: str, sector_filter: str = "") -> str:
    return f"stock_picks_{market}{_sector_suffix(sector_filter)}_{generated_at[:10]}_final"


def _persist_internal_detail_report(stem: str, markdown: str) -> Path:
    INTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    path = INTERNAL_DIR / f"{stem}.md"
    path.write_text(markdown, encoding="utf-8")
    return path


def _persist_stock_pick_payload(detail_path: Path, payload: Mapping[str, Any]) -> Path:
    payload_path = internal_sidecar_path(detail_path, "payload.json")
    payload_path.parent.mkdir(parents=True, exist_ok=True)
    payload_path.write_text(
        json.dumps(dict(payload or {}), ensure_ascii=False, indent=2, default=str) + "\n",
        encoding="utf-8",
    )
    return payload_path


def _persist_stock_pick_internal(detail_path: Path, markdown: str, payload: Mapping[str, Any]) -> tuple[Path, Path]:
    detail_path.parent.mkdir(parents=True, exist_ok=True)
    detail_path.write_text(str(markdown or ""), encoding="utf-8")
    payload_path = _persist_stock_pick_payload(detail_path, payload)
    return detail_path, payload_path


def _stock_pick_release_checker(
    editor_packet: Mapping[str, Any],
    editor_prompt: str,
) -> Callable[[str, str], Sequence[str]]:
    from src.commands.release_check import check_stock_pick_client_report

    return lambda markdown, source_text: check_stock_pick_client_report(
        markdown,
        source_text,
        editor_theme_playbook=editor_packet.get("theme_playbook") or {},
        editor_prompt_text=editor_prompt,
        event_digest_contract=editor_packet.get("event_digest") or {},
        what_changed_contract=editor_packet.get("what_changed") or {},
    )


def _merge_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    merged_top = []
    merged_watch = []
    merged_coverage = []
    generated_at = ""
    blind_spots = []
    for market in ("cn", "hk", "us"):
        payload = dict(payloads.get(market) or {})
        if payload and not generated_at:
            generated_at = str(payload.get("generated_at", ""))
        merged_top.extend(payload.get("top", []) or [])
        merged_watch.extend(payload.get("watch_positive", []) or [])
        merged_coverage.extend(payload.get("coverage_analyses", []) or payload.get("top", []) or [])
        blind_spots.extend(payload.get("blind_spots", []) or [])
    merged_top = sorted(merged_top, key=_rank_key, reverse=True)
    merged_watch = sorted(merged_watch, key=_rank_key, reverse=True)
    first = dict(next(iter(payloads.values())) or {})
    coverage_rows = merged_coverage or merged_top
    coverage = _coverage_summary(coverage_rows)
    market_proxy = dict(first.get("market_proxy") or {})
    proxy_contract = summarize_proxy_contracts_from_analyses(coverage_rows, market_proxy=market_proxy)
    return {
        "generated_at": generated_at,
        "top": merged_top,
        "watch_positive": merged_watch,
        "coverage_analyses": coverage_rows,
        "day_theme": dict(first.get("day_theme") or {}),
        "regime": dict(first.get("regime") or {}),
        "stock_pick_coverage": coverage,
        "market_proxy": market_proxy,
        "proxy_contract": proxy_contract,
        "data_coverage": {
            "news_mode": "mixed",
            "degraded": any(bool(dict(payload.get("data_coverage") or {}).get("degraded")) for payload in payloads.values()),
        },
        "market_label": "全市场",
        "blind_spots": list(dict.fromkeys(str(item).strip() for item in blind_spots if str(item).strip())),
    }


def _run_market(
    config: Mapping[str, Any],
    market: str,
    top_n: int,
    sector_filter: str,
    context: Optional[Mapping[str, Any]] = None,
    blind_spot_notes: Optional[list[str]] = None,
    *,
    enrich_finalists: bool = False,
) -> Dict[str, Any]:
    discovery_config = _client_final_discovery_config(config) if enrich_finalists else dict(config)
    if context is None:
        relevant_asset_types = {
            "cn": ["cn_stock", "cn_etf"],
            "hk": ["hk"],
            "us": ["us"],
            "all": ["cn_stock", "cn_etf", "hk", "us", "futures"],
        }.get(str(market).strip(), ["cn_stock", "cn_etf", "hk", "us", "futures"])
        context = build_market_context(config, relevant_asset_types=relevant_asset_types)
        context = _attach_shared_intel_news_report(
            context,
            config,
            query=_market_intel_query(market, sector_filter),
            note_prefix="stock_pick market intel: ",
        )
    payload = discover_stock_opportunities(discovery_config, top_n=top_n, market=market, sector_filter=sector_filter, context=context)
    if enrich_finalists:
        payload = _reenrich_stock_pick_finalists(payload, config, top_n=top_n, context=context)
    if blind_spot_notes:
        blind_spots = [str(item).strip() for item in list(payload.get("blind_spots") or []) if str(item).strip()]
        payload["blind_spots"] = list(dict.fromkeys([*blind_spot_notes, *blind_spots]))
    payload = enrich_payload_with_score_history(payload, market=market, sector_filter=sector_filter)
    payload["top"] = sorted(attach_portfolio_overlap_summaries(payload.get("top") or [], config), key=_rank_key, reverse=True)
    payload["watch_positive"] = sorted(
        attach_portfolio_overlap_summaries(payload.get("watch_positive") or [], config),
        key=_rank_key,
        reverse=True,
    )
    payload["coverage_analyses"] = sorted(
        attach_portfolio_overlap_summaries(payload.get("coverage_analyses") or [], config),
        key=_rank_key,
        reverse=True,
    )
    return _attach_featured_visuals(payload)


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("stock_pick")
    setup_logger("ERROR")
    base_config = load_config(args.config or None)
    if args.client_final:
        config, runtime_notes = _client_final_runtime_overrides(
            base_config,
            client_final=True,
            explicit_config_path=str(args.config or ""),
        )
    else:
        config, runtime_notes = _preview_runtime_overrides(
            base_config,
            explicit_config_path=str(args.config or ""),
        )
    sector_filter = args.sector.strip()
    try:
        if not args.client_final:
            payload = discover_stock_opportunities(config, top_n=args.top, market=args.market, sector_filter=sector_filter)
            payload = enrich_payload_with_score_history(payload, market=args.market, sector_filter=sector_filter)
            payload = _attach_featured_visuals(payload)
            print(OpportunityReportRenderer().render_stock_picks(payload))
            return

        if args.market == "all":
            shared_context = build_market_context(
                config,
                relevant_asset_types=["cn_stock", "cn_etf", "hk", "us", "futures"],
            )
            shared_context = _attach_shared_intel_news_report(
                shared_context,
                config,
                query=_market_intel_query(args.market, sector_filter),
                note_prefix="stock_pick market intel: ",
            )
            market_payloads = {
                market: _run_market(
                    config,
                    market,
                    args.top,
                    sector_filter,
                    context=shared_context,
                    blind_spot_notes=runtime_notes,
                    enrich_finalists=True,
                )
                for market in ("cn", "hk", "us")
            }
            for market, payload in market_payloads.items():
                detailed = OpportunityReportRenderer().render_stock_picks(payload)
                detail_path = INTERNAL_DIR / f"{_internal_detail_stem(market, str(payload.get('generated_at', '')), sector_filter)}.md"
                _persist_stock_pick_internal(
                    detail_path,
                    detailed,
                    payload,
                )
            client_payload = _attach_featured_visuals(_merge_payloads(market_payloads), render_theme_variants=True)
            factor_contract = _factor_contract_summary(
                [
                    analysis
                    for market_payload in market_payloads.values()
                    for analysis in list(market_payload.get("coverage_analyses") or market_payload.get("top") or [])
                ]
            )
            source_path, _payload_path = _persist_stock_pick_internal(
                INTERNAL_DIR / f"{_internal_merged_stem(str(client_payload.get('generated_at', '')), sector_filter)}.md",
                OpportunityReportRenderer().render_stock_picks(client_payload),
                client_payload,
            )
            catalyst_review_path = internal_sidecar_path(source_path, "catalyst_web_review.md")
            review_lookup = load_catalyst_web_review(catalyst_review_path)
            if review_lookup:
                for key in ("coverage_analyses", "top"):
                    client_payload[key] = [
                        attach_catalyst_web_review_to_analysis(item, review_lookup)
                        for item in list(client_payload.get(key) or [])
                    ]
            client_markdown = ClientReportRenderer().render_stock_picks_detailed(client_payload)
            target_path = FINAL_DIR / f"{_final_stem(str(client_payload.get('generated_at', '')), sector_filter)}.md"

            try:
                from src.commands.release_check import check_stock_pick_client_report

                editor_packet = build_stock_pick_editor_packet(client_payload)
                editor_prompt = render_financial_editor_prompt(editor_packet)
                catalyst_packet = build_catalyst_web_review_packet(
                    report_type="stock_pick",
                    subject=f"stock_pick {client_payload.get('generated_at', '')[:10]}",
                    generated_at=str(client_payload.get("generated_at", "")),
                    analyses=list(client_payload.get("coverage_analyses") or client_payload.get("top") or []),
                )
                text_sidecars = {
                    "editor_prompt": (
                        internal_sidecar_path(source_path, "editor_prompt.md"),
                        editor_prompt,
                    )
                }
                json_sidecars = {
                    "editor_payload": (
                        internal_sidecar_path(source_path, "editor_payload.json"),
                        editor_packet,
                    )
                }
                if list(catalyst_packet.get("items") or []):
                    text_sidecars.update(
                        {
                            "catalyst_web_review_prompt": (
                                internal_sidecar_path(source_path, "catalyst_web_review_prompt.md"),
                                render_catalyst_web_review_prompt(catalyst_packet),
                            ),
                            "catalyst_web_review": (
                                internal_sidecar_path(source_path, "catalyst_web_review.md"),
                                render_catalyst_web_review_scaffold(catalyst_packet),
                            ),
                        }
                    )
                    json_sidecars.update(
                        {
                            "catalyst_web_review_payload": (
                                internal_sidecar_path(source_path, "catalyst_web_review_payload.json"),
                                catalyst_packet,
                            )
                        }
                    )
                elif catalyst_review_path.exists():
                    text_sidecars["catalyst_web_review"] = (
                        catalyst_review_path,
                        catalyst_review_path.read_text(encoding="utf-8"),
                    )
                bundle = finalize_client_markdown(
                    report_type="stock_pick",
                    client_markdown=client_markdown,
                    markdown_path=target_path,
                    detail_markdown=source_path.read_text(encoding="utf-8"),
                    detail_path=source_path,
                    extra_manifest={
                        "market": "all",
                        "factor_contract": factor_contract,
                        "proxy_contract": dict(client_payload.get("proxy_contract") or {}),
                        "theme_playbook_contract": summarize_theme_playbook_contract(editor_packet.get("theme_playbook") or {}),
                        "event_digest_contract": summarize_event_digest_contract(editor_packet.get("event_digest") or {}),
                        "what_changed_contract": summarize_what_changed_contract(editor_packet.get("what_changed") or {}),
                    },
                    release_checker=_stock_pick_release_checker(editor_packet, editor_prompt),
                    text_sidecars=text_sidecars,
                    json_sidecars=json_sidecars,
                )
            except Exception as exc:
                raise SystemExit(str(exc))

            print(client_markdown)
            for index, line in enumerate(exported_bundle_lines(bundle)):
                print(f"\n{line}" if index == 0 else line)
            return

        payload = _run_market(config, args.market, args.top, sector_filter, blind_spot_notes=runtime_notes, enrich_finalists=True)
        payload = _attach_featured_visuals(payload, render_theme_variants=True)
        detailed = OpportunityReportRenderer().render_stock_picks(payload)
        detail_path, _payload_path = _persist_stock_pick_internal(
            INTERNAL_DIR / f"{_internal_detail_stem(args.market, str(payload.get('generated_at', '')), sector_filter)}.md",
            detailed,
            payload,
        )
        catalyst_review_path = internal_sidecar_path(detail_path, "catalyst_web_review.md")
        review_lookup = load_catalyst_web_review(catalyst_review_path)
        if review_lookup:
            for key in ("coverage_analyses", "top"):
                payload[key] = [
                    attach_catalyst_web_review_to_analysis(item, review_lookup)
                    for item in list(payload.get(key) or [])
                ]
        client_markdown = ClientReportRenderer().render_stock_picks_detailed(payload)
        target_path = FINAL_DIR / f"{_market_final_stem(args.market, str(payload.get('generated_at', '')), sector_filter)}.md"
        factor_contract = _factor_contract_summary(list(payload.get("coverage_analyses") or payload.get("top") or []))
        editor_packet = build_stock_pick_editor_packet(payload)
        editor_prompt = render_financial_editor_prompt(editor_packet)

        catalyst_packet = build_catalyst_web_review_packet(
            report_type="stock_pick",
            subject=f"stock_pick {payload.get('generated_at', '')[:10]}",
            generated_at=str(payload.get("generated_at", "")),
            analyses=list(payload.get("coverage_analyses") or payload.get("top") or []),
        )
        text_sidecars = {
            "editor_prompt": (
                internal_sidecar_path(detail_path, "editor_prompt.md"),
                editor_prompt,
            )
        }
        json_sidecars = {
            "editor_payload": (
                internal_sidecar_path(detail_path, "editor_payload.json"),
                editor_packet,
            )
        }
        if list(catalyst_packet.get("items") or []):
            text_sidecars.update(
                {
                    "catalyst_web_review_prompt": (
                        internal_sidecar_path(detail_path, "catalyst_web_review_prompt.md"),
                        render_catalyst_web_review_prompt(catalyst_packet),
                    ),
                    "catalyst_web_review": (
                        internal_sidecar_path(detail_path, "catalyst_web_review.md"),
                        render_catalyst_web_review_scaffold(catalyst_packet),
                    ),
                }
            )
            json_sidecars.update(
                {
                    "catalyst_web_review_payload": (
                        internal_sidecar_path(detail_path, "catalyst_web_review_payload.json"),
                        catalyst_packet,
                    )
                }
            )
        elif catalyst_review_path.exists():
            text_sidecars["catalyst_web_review"] = (
                catalyst_review_path,
                catalyst_review_path.read_text(encoding="utf-8"),
            )
        bundle = finalize_client_markdown(
            report_type="stock_pick",
            client_markdown=client_markdown,
            markdown_path=target_path,
            detail_markdown=detailed,
            detail_path=detail_path,
            extra_manifest={
                "market": args.market,
                "factor_contract": factor_contract,
                "proxy_contract": dict(payload.get("proxy_contract") or {}),
                "theme_playbook_contract": summarize_theme_playbook_contract(editor_packet.get("theme_playbook") or {}),
                "event_digest_contract": summarize_event_digest_contract(editor_packet.get("event_digest") or {}),
                "what_changed_contract": summarize_what_changed_contract(editor_packet.get("what_changed") or {}),
            },
            release_checker=_stock_pick_release_checker(editor_packet, editor_prompt) if args.market == "cn" else None,
            text_sidecars=text_sidecars,
            json_sidecars=json_sidecars,
        )

        print(client_markdown)
        for index, line in enumerate(exported_bundle_lines(bundle)):
            print(f"\n{line}" if index == 0 else line)
    finally:
        close_yfinance_runtime_caches()


if __name__ == "__main__":
    main()
