"""Daily ETF recommendation command."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from copy import deepcopy
from pathlib import Path
import re
import threading
from typing import Any, Dict, List, Mapping, Sequence

from src.commands.pick_history import enrich_pick_payload_with_score_history, grade_pick_delivery, summarize_pick_coverage
from src.commands.pick_visuals import attach_visuals_to_analyses
from src.commands.final_runner import finalize_client_markdown, internal_sidecar_path
from src.commands.report_guard import ensure_report_task_registered, exported_bundle_lines
from src.commands.release_check import check_generic_client_report
from src.collectors.fund_profile import FundProfileCollector
from src.collectors.news import NewsCollector
from src.output import ClientReportRenderer, OpportunityReportRenderer
from src.output.catalyst_web_review import (
    attach_catalyst_web_review_to_analysis,
    build_catalyst_web_review_packet,
    load_catalyst_web_review,
    render_catalyst_web_review_prompt,
    render_catalyst_web_review_scaffold,
)
from src.output.editor_payload import (
    _attach_strategy_background_confidence,
    build_etf_pick_editor_packet,
    render_financial_editor_prompt,
    summarize_theme_playbook_contract,
    summarize_what_changed_contract,
)
from src.output.event_digest import summarize_event_digest_contract
from src.output.opportunity_report import _dimension_summary_text
from src.output.client_report import _fund_profile_sections, _pick_horizon_profile
from src.output.pick_ranking import portfolio_overlap_bonus, score_band, strategy_confidence_priority
from src.processors.factor_meta import summarize_factor_contracts_from_analyses
from src.processors.portfolio_actions import attach_portfolio_overlap_summaries
from src.processors.opportunity_engine import (
    _client_safe_issue,
    analyze_opportunity,
    build_market_context,
    discover_opportunities,
    refresh_etf_analysis_report_fields,
)
from src.utils.fund_taxonomy import taxonomy_from_analysis, taxonomy_rows
from src.utils.config import load_config, resolve_project_path
from src.utils.data import load_watchlist
from src.utils.logger import setup_logger
from src.utils.market import close_yfinance_runtime_caches

SNAPSHOT_PATH = resolve_project_path("data/etf_pick_score_history.json")
MODEL_VERSION = "etf-pick-2026-03-14-candlestick-v4"
MODEL_CHANGELOG = [
    "ETF 推荐现在记录同日基准版和重跑快照，后续重跑会展示分数变化而不是静态覆盖旧稿。",
    "催化面在新闻/事件覆盖降级时会按最近一次有效快照做衰减回退，避免把 ETF 催化打成假阴性。",
    "客户稿和内部详细稿都会披露扫描池来源、覆盖率和分母定义，外审门禁同步要求这些章节存在。",
    "技术面新增 `量价/动量背离` 因子，按最近两组确认摆点检查 RSI / MACD / OBV 与价格是否出现顶/底背离。",
    "K 线形态从“单根 K”升级到“最近 1-3 根组合形态”，会识别吞没、星形、三兵三鸦等常见信号，并结合前序 5 日趋势过滤误报。",
]

_ETF_THEME_NOISE_TOKENS = (
    "ETF",
    "联接",
    "基金",
    "指数",
    "主题",
    "行业",
    "A股",
    "中证",
    "国证",
    "上证",
    "深证",
    "申万",
    "科创板",
    "创业板",
)
_ETF_THEME_EXPANSION_HINTS = (
    (("半导体", "芯片"), ["AI算力", "晶圆厂", "设备材料", "capex"]),
    (("算力", "AI"), ["数据中心", "服务器", "GPU", "capex"]),
    (("光模块", "光通信"), ["AI算力", "数据中心", "800G", "CPO"]),
)
_LOW_SIGNAL_ETF_NEWS_TOKENS = (
    "净值",
    "开盘",
    "收盘",
    "重仓股",
    "成交额",
    "换手率",
    "净申购",
    "净流入",
    "净流出",
    "半日净申购",
    "半日成交额",
    "资金逆势加码",
    "涨超",
    "跌超",
    "开盘涨",
    "开盘跌",
    "涨0.00%",
    "跌0.00%",
    "涨幅",
    "跌幅",
    "溢价",
    "折价",
    "费率",
    "规模",
    "份额",
)
_QUOTE_NOISE_ETF_NEWS_TOKENS = (
    "净值",
    "开盘",
    "收盘",
    "重仓股",
    "涨0.00%",
    "跌0.00%",
)
_GENERIC_MARKET_ETF_NEWS_TOKENS = (
    "市场回暖",
    "回暖信号",
    "关注三个方向",
    "关注三条主线",
    "关注三大方向",
    "四月份关注",
    "四月关注",
    "布局三个方向",
    "布局三条主线",
    "风格切换",
    "市场风格",
    "三大方向",
)
_GENERIC_MARKET_ETF_NEWS_SOURCES = (
    "财富号",
)
_THEME_CATALYST_TOKENS = (
    "AI",
    "人工智能",
    "算力",
    "半导体",
    "芯片",
    "晶圆",
    "设备",
    "材料",
    "capex",
    "扩产",
    "订单",
    "中标",
    "政策",
    "补贴",
    "关税",
    "国产替代",
    "景气",
    "需求",
    "库存",
    "价格",
    "出货",
    "服务器",
    "数据中心",
    "量产",
    "制程",
    "制程升级",
    "晶圆厂",
    "资本开支",
    "扩产",
    "投产",
    "产能",
    "设备支出",
    "供需",
    "半导体设备",
)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select today's ETF pick from the Tushare ETF universe.")
    parser.add_argument("theme", nargs="?", default="", help="Optional ETF theme filter, e.g. 红利 / 黄金 / 电网 / 能化")
    parser.add_argument("--top", type=int, default=8, help="Number of ETF analyses to consider")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--client-final", action="store_true", help="Render and persist customer-facing final markdown/pdf")
    return parser


def _score_of(analysis: Dict[str, Any], key: str) -> float:
    return float(dict(analysis.get("dimensions", {}).get(key) or {}).get("score") or 0)


def _table(headers: Sequence[str], rows: Sequence[Sequence[str]]) -> List[str]:
    def _escape(value: Any) -> str:
        return str(value).replace("|", "\\|").replace("\n", "<br>")

    lines = [
        "| " + " | ".join(_escape(header) for header in headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(_escape(cell) for cell in row) + " |")
    return lines


def _dimension_rows(analysis: Dict[str, Any]) -> List[List[str]]:
    rows: List[List[str]] = []
    labels = [
        ("technical", "技术面"),
        ("fundamental", "基本面"),
        ("catalyst", "催化面"),
        ("relative_strength", "相对强弱"),
        ("chips", "筹码结构"),
        ("risk", "风险特征"),
        ("seasonality", "季节/日历"),
        ("macro", "宏观敏感度"),
    ]
    for key, label in labels:
        dimension = dict(analysis.get("dimensions", {}).get(key) or {})
        score = dimension.get("score")
        max_score = dimension.get("max_score", 100)
        display_name = str(dimension.get("display_name", label))
        display = "—" if score is None else f"{score}/{max_score}"
        reason = _dimension_summary_text(key, dimension)
        if key == "chips":
            display_name = "筹码结构（辅助项）"
            display = "辅助项"
            if reason and "主排序不直接使用" not in reason:
                reason = f"{reason} 当前主排序不直接使用这项。".strip()
        rows.append([display_name, display, reason])
    return rows


def _market_event_rows(analysis: Mapping[str, Any]) -> List[List[str]]:
    rows = [list(row) for row in list(analysis.get("market_event_rows") or []) if isinstance(row, (list, tuple)) and row]
    if rows:
        return rows
    relative_strength = dict(dict(analysis.get("dimensions") or {}).get("relative_strength") or {})
    summary = str(relative_strength.get("summary", "")).strip()
    match = re.search(r"板块涨跌幅\s*([+-]?\d+(?:\.\d+)?)%", summary)
    if match is None:
        return []
    move_value = float(match.group(1))
    metadata = dict(analysis.get("metadata") or {})
    board_name = str(metadata.get("sector", "")).strip() or str(analysis.get("name", "")).strip()
    if not board_name:
        return []
    strength = "高" if move_value >= 3 else ("中" if move_value >= 1 else "低")
    conclusion = (
        f"偏利多，先把 `{board_name}` 当盘面共振线索，继续看价格和成交能否扩散。"
        if move_value >= 0
        else f"偏谨慎，先看 `{board_name}` 是否继续走弱，不把它直接当成动作催化。"
    )
    return [
        [
            str(analysis.get("generated_at", ""))[:10],
            f"主题/盘面跟踪：{board_name}（板块涨跌幅 {move_value:+.2f}%）",
            "相对强弱/盘面",
            strength,
            board_name,
            "",
            "盘面共振",
            conclusion,
        ]
    ]


def _timed_value(loader, *, fallback: Any, timeout_seconds: float) -> Any:
    result: Dict[str, Any] = {}

    def _runner() -> None:
        try:
            result["value"] = loader()
        except BaseException:
            result["value"] = fallback

    worker = threading.Thread(target=_runner, name="etf_pick_news_backfill", daemon=True)
    worker.start()
    worker.join(timeout_seconds)
    if worker.is_alive():
        return fallback
    return result.get("value", fallback)


def _etf_theme_terms(analysis: Mapping[str, Any]) -> List[str]:
    metadata = dict(analysis.get("metadata") or {})
    values = [
        metadata.get("tracked_index_name"),
        metadata.get("index_framework_label"),
        dict(metadata.get("index_topic_bundle") or {}).get("index_snapshot", {}).get("index_name"),
        analysis.get("benchmark_name"),
        analysis.get("name"),
    ]
    raw_terms: List[str] = []
    for value in values:
        text = str(value or "").strip()
        if not text:
            continue
        normalized = text
        for token in _ETF_THEME_NOISE_TOKENS:
            normalized = normalized.replace(token, " ")
        normalized = re.sub(r"[()/,_\-]+", " ", normalized)
        normalized = re.sub(r"\s+", " ", normalized).strip()
        if normalized and normalized not in raw_terms:
            raw_terms.append(normalized)
    for node in list(metadata.get("chain_nodes") or []):
        text = str(node or "").strip()
        if text and text not in raw_terms:
            raw_terms.append(text)
    return [item for item in raw_terms if len(item) >= 2][:6]


def _etf_theme_expansion_terms(analysis: Mapping[str, Any]) -> List[str]:
    theme_terms = _etf_theme_terms(analysis)
    expansions: List[str] = []
    joined = " ".join(theme_terms)
    for triggers, hints in _ETF_THEME_EXPANSION_HINTS:
        if any(token in joined for token in triggers):
            for hint in hints:
                if hint not in expansions:
                    expansions.append(hint)
    return expansions[:4]


def _is_low_value_etf_news_item(item: Mapping[str, Any], analysis: Mapping[str, Any]) -> bool:
    title = str(dict(item or {}).get("title") or "").strip()
    if not title:
        return False
    if not any(token in title for token in ("ETF", "基金", "联接")):
        return False
    if any(token in title for token in _QUOTE_NOISE_ETF_NEWS_TOKENS):
        return True
    if not any(token in title for token in _LOW_SIGNAL_ETF_NEWS_TOKENS):
        return False
    return True


def _theme_specific_terms(analysis: Mapping[str, Any]) -> List[str]:
    metadata = dict(analysis.get("metadata") or {})
    terms: List[str] = []
    raw_terms = [
        *_etf_theme_terms(analysis),
        *_etf_theme_expansion_terms(analysis),
        str(metadata.get("tracked_index_name") or "").strip(),
        str(metadata.get("industry_framework_label") or "").strip(),
        str(metadata.get("index_top_constituent_name") or "").strip(),
        *[str(item).strip() for item in list(metadata.get("chain_nodes") or [])],
    ]
    for raw in raw_terms:
        text = str(raw or "").strip()
        if not text:
            continue
        for part in re.split(r"[\s/,_()\-]+", text):
            cleaned = part.strip()
            if len(cleaned) < 2 or cleaned in {"综合", "科技", "信息技术"}:
                continue
            if cleaned not in terms:
                terms.append(cleaned)
    return terms[:16]


def _is_generic_market_etf_news_item(item: Mapping[str, Any], analysis: Mapping[str, Any]) -> bool:
    row = dict(item or {})
    title = str(row.get("title") or "").strip()
    if not title:
        return False
    source = str(row.get("source") or "").strip()
    generic_title = any(token in title for token in _GENERIC_MARKET_ETF_NEWS_TOKENS)
    generic_source = any(token in source for token in _GENERIC_MARKET_ETF_NEWS_SOURCES)
    if not generic_title and not generic_source:
        return False
    if any(term in title for term in _theme_specific_terms(analysis)):
        return False
    if sum(1 for token in _THEME_CATALYST_TOKENS if token in title) >= 2:
        return False
    return True


def _curate_etf_news_items(items: Sequence[Mapping[str, Any]], analysis: Mapping[str, Any]) -> List[Dict[str, Any]]:
    curated: List[Dict[str, Any]] = []
    for item in list(items or []):
        row = dict(item or {})
        if _is_low_value_etf_news_item(row, analysis):
            continue
        if _is_generic_market_etf_news_item(row, analysis):
            continue
        curated.append(row)
    return curated


def _etf_news_query_groups(analysis: Mapping[str, Any]) -> List[List[str]]:
    metadata = dict(analysis.get("metadata") or {})
    fund_profile = dict(analysis.get("fund_profile") or {})
    name = str(analysis.get("name") or metadata.get("name") or "").strip()
    clean_name = name.replace("ETF", "").replace("联接", "").replace("基金", "").strip() or name
    analysis_benchmark_name = str(analysis.get("benchmark_name") or "").strip()
    tracked_index_name = str(
        metadata.get("tracked_index_name")
        or metadata.get("benchmark")
        or metadata.get("index_framework_label")
        or dict(metadata.get("index_topic_bundle") or {}).get("index_snapshot", {}).get("index_name")
        or metadata.get("benchmark_name")
        or metadata.get("index_name")
        or (
            analysis_benchmark_name
            if analysis_benchmark_name and not analysis_benchmark_name.endswith("ETF")
            else ""
        )
        or ""
    ).strip()
    sector = str(metadata.get("sector") or metadata.get("category") or "").strip()
    industry_framework = str(metadata.get("industry_framework_label") or "").strip()
    index_top_constituent = str(metadata.get("index_top_constituent_name") or "").strip()
    chain_nodes = [str(item).strip() for item in list(metadata.get("chain_nodes") or []) if str(item).strip()]
    holding_names = [
        str(item.get("股票名称", "")).strip()
        for item in list(fund_profile.get("top_holdings") or [])[:3]
        if str(item.get("股票名称", "")).strip()
    ]
    theme_terms = _etf_theme_terms(analysis)
    expansion_terms = _etf_theme_expansion_terms(analysis)
    primary_theme = theme_terms[0] if theme_terms else ""

    groups: List[List[str]] = []
    seen: set[tuple[str, ...]] = set()

    def _add_group(*terms: str) -> None:
        cleaned = tuple(term.strip() for term in terms if str(term).strip())
        if not cleaned or cleaned in seen:
            return
        seen.add(cleaned)
        groups.append(list(cleaned))

    if tracked_index_name:
        _add_group(tracked_index_name, primary_theme or clean_name)
    if primary_theme:
        _add_group(primary_theme, tracked_index_name or industry_framework or clean_name)
    for hint in expansion_terms[:3]:
        if primary_theme:
            _add_group(primary_theme, hint)
        elif tracked_index_name:
            _add_group(tracked_index_name, hint)
    if industry_framework:
        _add_group(industry_framework, tracked_index_name or primary_theme or clean_name)
    if primary_theme and sector not in {"综合", "科技", "信息技术"}:
        _add_group(primary_theme, sector)
    if clean_name:
        _add_group(clean_name, primary_theme or sector)
    if sector and sector not in {"综合", "科技", "信息技术"}:
        _add_group(f"{sector} A股", primary_theme or sector)
    if index_top_constituent:
        _add_group(index_top_constituent, tracked_index_name or primary_theme or clean_name)
    for holding_name in holding_names[:2]:
        _add_group(holding_name, tracked_index_name or primary_theme or sector or clean_name)
    for node in chain_nodes[:3]:
        _add_group(node, tracked_index_name or clean_name or name)
    return groups[:8]


def _backfill_etf_news_report(
    analysis: Mapping[str, Any],
    *,
    config: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    existing = dict(analysis.get("news_report") or {})
    current_items = _curate_etf_news_items(list(existing.get("items") or []), analysis)
    if current_items != list(existing.get("items") or []):
        existing["items"] = current_items
    linked_items = [item for item in current_items if str(dict(item).get("link") or "").strip()]
    if len(linked_items) >= 2 or (len(current_items) >= 3 and len(linked_items) >= 1):
        existing["lines"] = [str(dict(item).get("title", "")).strip() for item in current_items[:4] if str(dict(item).get("title", "")).strip()]
        existing["source_list"] = sorted({str(dict(item).get("source", "")).strip() for item in current_items if str(dict(item).get("source", "")).strip()})
        return existing

    query_groups = _etf_news_query_groups(analysis)
    if not query_groups:
        return existing

    def _loader() -> Dict[str, Any]:
        preferred_sources = ["财联社", "证券时报", "上海证券报", "中国证券报", "Reuters", "Bloomberg"]
        active_query_groups = list(query_groups[:4])
        collector_config = deepcopy(dict(config or {}))
        collector_config["news_topic_search_enabled"] = True
        collector_config.setdefault("news_feeds_file", "config/news_feeds.briefing_light.yaml")
        collector = NewsCollector(collector_config)
        merged = list(current_items)
        query_terms = [item for group in active_query_groups for item in group]
        notes: List[str] = []

        try:
            tushare_hits = collector.get_market_intelligence(query_terms, limit=6, recent_days=7)
        except Exception:
            tushare_hits = []
        if tushare_hits:
            merged.extend(tushare_hits)
            notes.append("已按 Tushare 市场情报补充。")

        try:
            search_hits = collector.search_by_keyword_groups(
                active_query_groups,
                preferred_sources=preferred_sources,
                limit=6,
                recent_days=5,
            )
        except Exception:
            search_hits = []
        if search_hits:
            merged.extend(search_hits)
            notes.append("已按 ETF 名称/指数/主题搜索补充。")

        merged = _curate_etf_news_items(merged, analysis)

        ranked = collector._rank_items(
            collector._filter_candidate_items(merged, recent_days=7),
            preferred_sources=preferred_sources + ["Tushare"],
            query_keywords=query_terms,
        )
        selected = collector._diversify_items(ranked, 6)
        if not selected:
            return existing
        return {
            "mode": "live",
            "items": selected,
            "all_items": ranked,
            "lines": collector._live_lines(selected),
            "source_list": sorted(collector._present_sources(selected)),
            "note": "ETF 外部情报已按名称/指数/主题/成分线索回填。" + "".join(notes),
        }

    return dict(
        _timed_value(
            _loader,
            fallback=existing,
            timeout_seconds=float(dict(config or {}).get("etf_news_backfill_timeout_seconds", 8) or 8),
        )
    )


def _hydrate_selected_etf_profiles(
    analyses: Sequence[Mapping[str, Any]],
    *,
    config: Mapping[str, Any] | None = None,
    limit: int = 3,
    context: Mapping[str, Any] | None = None,
) -> List[Dict[str, Any]]:
    hydrated: List[Dict[str, Any]] = []
    collector = FundProfileCollector(config or {})
    max_items = max(int(limit or 0), 0)
    full_reanalysis_limit = max(int(dict(config or {}).get("etf_full_reanalysis_limit", 0) or 0), 0)
    full_reanalysis_context: Dict[str, Any] | None = dict(context or {}) if context else None
    full_reanalysis_config = dict(config or {})
    full_reanalysis_config["skip_fund_profile"] = False
    full_reanalysis_config["etf_fund_profile_mode"] = "full"
    for index, raw in enumerate(list(analyses or [])):
        updated = dict(raw)
        if index < max_items:
            symbol = str(updated.get("symbol") or "").strip()
            if symbol:
                try:
                    profile = collector.collect_profile(symbol, asset_type="cn_etf", profile_mode="full")
                except Exception:
                    profile = {}
                if profile:
                    updated["fund_profile"] = profile
                    if index < full_reanalysis_limit:
                        if full_reanalysis_context is None:
                            full_reanalysis_context = build_market_context(full_reanalysis_config, relevant_asset_types=["cn_etf", "futures"])
                        metadata = dict(updated.get("metadata") or {})
                        try:
                            rerun = analyze_opportunity(
                                symbol,
                                "cn_etf",
                                full_reanalysis_config,
                                context={**dict(full_reanalysis_context), "config": dict(full_reanalysis_config), "runtime_caches": {}},
                                metadata_override={
                                    "name": str(metadata.get("name") or updated.get("name") or symbol),
                                    "sector": str(metadata.get("sector") or "综合"),
                                    "chain_nodes": list(metadata.get("chain_nodes") or []),
                                    "region": str(metadata.get("region") or "CN"),
                                    "in_watchlist": bool(metadata.get("in_watchlist")),
                                },
                            )
                        except Exception:
                            rerun = {}
                        if rerun:
                            updated = {**updated, **rerun}
                            updated["fund_profile"] = dict(updated.get("fund_profile") or profile)
                        else:
                            updated = refresh_etf_analysis_report_fields(updated, config=full_reanalysis_config)
                    else:
                        updated = refresh_etf_analysis_report_fields(updated, config=full_reanalysis_config)
        hydrated.append(updated)
    return hydrated


def _etf_structure_rank_bonus(analysis: Mapping[str, Any]) -> float:
    asset_type = str(analysis.get("asset_type", "")).strip()
    if asset_type not in {"cn_etf", "cn_fund"}:
        return 0.0
    metadata = dict(analysis.get("metadata") or {})
    bonus = 0.0
    if str(metadata.get("index_framework_label", "")).strip() or str(analysis.get("benchmark_name", "")).strip():
        bonus += 1.5
    if str(metadata.get("industry_framework_label", "")).strip():
        bonus += 0.8
    if metadata.get("index_top_weight_sum") not in (None, "", []):
        bonus += 1.0
    if str(metadata.get("index_top_constituent_name", "")).strip():
        bonus += 0.7
    return bonus


def _etf_share_flow_rank_bonus(analysis: Mapping[str, Any]) -> float:
    asset_type = str(analysis.get("asset_type", "")).strip()
    if asset_type not in {"cn_etf", "cn_fund"}:
        return 0.0
    metadata = dict(analysis.get("metadata") or {})
    try:
        share_value = float(metadata.get("etf_share_change"))
    except (TypeError, ValueError):
        share_value = None
    try:
        share_pct = float(metadata.get("etf_share_change_pct"))
    except (TypeError, ValueError):
        share_pct = None
    if share_value is None and share_pct is None:
        return 0.0
    if (share_value is not None and share_value >= 5) or (share_pct is not None and share_pct >= 3):
        return 3.0
    if (share_value is not None and share_value > 0) or (share_pct is not None and share_pct > 0):
        return 1.8
    if (share_value is not None and share_value <= -5) or (share_pct is not None and share_pct <= -3):
        return -2.0
    if share_value is not None and share_value < 0:
        return -1.0
    return 0.0


def _etf_share_snapshot_note(analysis: Mapping[str, Any]) -> str:
    asset_type = str(analysis.get("asset_type", "")).strip()
    if asset_type not in {"cn_etf", "cn_fund"}:
        return ""
    metadata = dict(analysis.get("metadata") or {})
    fund_profile = dict(analysis.get("fund_profile") or {})
    etf_snapshot = dict(fund_profile.get("etf_snapshot") or {})
    share_as_of = str(etf_snapshot.get("share_as_of", metadata.get("share_as_of", ""))).strip()
    if not share_as_of:
        return ""

    try:
        share_value = float(etf_snapshot.get("etf_share_change", metadata.get("etf_share_change")))
    except (TypeError, ValueError):
        share_value = None
    try:
        share_pct = float(etf_snapshot.get("etf_share_change_pct", metadata.get("etf_share_change_pct")))
    except (TypeError, ValueError):
        share_pct = None
    if share_value is not None or share_pct is not None:
        return ""

    total_share_source = "total_share_yi" if etf_snapshot.get("total_share_yi") not in (None, "") else "total_share"
    total_share_value = etf_snapshot.get("total_share_yi", etf_snapshot.get("total_share"))
    try:
        total_share_value = float(total_share_value)
    except (TypeError, ValueError):
        total_share_value = None
    if total_share_value is None:
        return ""
    name = str(analysis.get("name", "—")).strip() or "—"
    symbol = str(analysis.get("symbol", "—")).strip() or "—"
    display_share = total_share_value if total_share_source == "total_share_yi" else total_share_value / 10000.0
    return (
        f"`{name} ({symbol})` 份额快照仍只有单日口径：截至 `{share_as_of}` 总份额约 `{display_share:.2f} 亿份`；"
        "先按观察处理，不把申赎方向写死。"
    )


def _etf_share_snapshot_notes(analyses: Sequence[Mapping[str, Any]]) -> List[str]:
    notes: List[str] = []
    for item in analyses:
        note = _etf_share_snapshot_note(item)
        if note and note not in notes:
            notes.append(note)
    return notes


def _rank_score(analysis: Dict[str, Any]) -> float:
    base_score = (
        _score_of(analysis, "technical") * 0.22
        + _score_of(analysis, "fundamental") * 0.18
        + _score_of(analysis, "catalyst") * 0.18
        + _score_of(analysis, "relative_strength") * 0.22
        + _score_of(analysis, "risk") * 0.12
        + _score_of(analysis, "macro") * 0.08
    )
    return base_score + _etf_structure_rank_bonus(analysis) + _etf_share_flow_rank_bonus(analysis)


def _rank_key(analysis: Mapping[str, Any]) -> tuple[float, float, float, float, float, float]:
    rank_score = _rank_score(dict(analysis))
    return (
        0.0 if bool(analysis.get("history_fallback_mode")) else 1.0,
        float(int(analysis.get("rating", {}).get("rank", 0) or 0)),
        float(score_band(rank_score)),
        float(portfolio_overlap_bonus(analysis)),
        rank_score,
        3 - strategy_confidence_priority(analysis),
        _score_of(dict(analysis), "relative_strength"),
        _score_of(dict(analysis), "catalyst"),
    )


def _etf_structure_reason_lines(analysis: Mapping[str, Any]) -> List[str]:
    asset_type = str(analysis.get("asset_type", "")).strip()
    if asset_type not in {"cn_etf", "cn_fund"}:
        return []
    metadata = dict(analysis.get("metadata") or {})
    fund_profile = dict(analysis.get("fund_profile") or {})
    etf_snapshot = dict(fund_profile.get("etf_snapshot") or {})
    fund_factor_snapshot = dict(fund_profile.get("fund_factor_snapshot") or {})
    reasons: List[str] = []

    index_name = str(
        metadata.get("index_framework_label")
        or metadata.get("tracked_index_name")
        or metadata.get("benchmark_name")
        or metadata.get("index_name")
        or etf_snapshot.get("index_name")
        or analysis.get("benchmark_name")
        or ""
    ).strip()
    industry_label = str(metadata.get("industry_framework_label", "")).strip()
    top_constituent = str(metadata.get("index_top_constituent_name", "")).strip() or str(metadata.get("index_top_constituent_symbol", "")).strip()
    top_weight_sum = metadata.get("index_top_weight_sum")
    if index_name:
        if top_weight_sum not in (None, "", []):
            try:
                weight_value = float(top_weight_sum)
                reasons.append(
                    f"跟踪指数 `{index_name}` 的结构已经清楚，前排权重合计约 `{weight_value:.1f}%`"
                    + (f"，核心成分先看 `{top_constituent}`。" if top_constituent else "。")
                )
            except (TypeError, ValueError):
                reasons.append(f"跟踪指数 `{index_name}` 已有标准指数框架，先按指数暴露理解这只 ETF。")
        else:
            reasons.append(f"跟踪指数 `{index_name}` 已有标准指数框架，先按指数暴露理解这只 ETF。")
    if industry_label:
        reasons.append(f"行业框架当前更接近 `{industry_label}`，不再只靠泛主题词猜主线。")

    try:
        share_value = float(etf_snapshot.get("etf_share_change", metadata.get("etf_share_change")))
    except (TypeError, ValueError):
        share_value = None
    try:
        share_pct = float(etf_snapshot.get("etf_share_change_pct", metadata.get("etf_share_change_pct")))
    except (TypeError, ValueError):
        share_pct = None
    share_as_of = str(etf_snapshot.get("share_as_of", metadata.get("share_as_of", ""))).strip()
    if share_value is not None:
        pct_text = f"（{share_pct:+.2f}%）" if share_pct is not None else ""
        if share_value > 0:
            reasons.append(f"份额最近净创设 `{share_value:+.2f} 亿份`{pct_text}，说明有场外申购在配合。")
        elif share_value < 0:
            reasons.append(f"份额最近净赎回 `{share_value:+.2f} 亿份`{pct_text}，价格变化还没完全得到份额流入确认。")
        else:
            reasons.append("份额最近基本持平，当前更多看指数主线和价格确认。")
        if share_as_of:
            reasons[-1] = f"{reasons[-1]} 口径日期 `{share_as_of}`。"
    else:
        total_share_yi = etf_snapshot.get("total_share_yi")
        try:
            total_share_yi_value = float(total_share_yi)
        except (TypeError, ValueError):
            total_share_yi_value = None
        if total_share_yi_value is not None and share_as_of:
            reasons.append(
                f"份额快照已接上：截至 `{share_as_of}` 总份额约 `{total_share_yi_value:.2f} 亿份`；"
                "当前缺前一日快照，先不把申赎方向写死。"
            )

    factor_trend = str(fund_factor_snapshot.get("trend_label", "")).strip()
    if factor_trend:
        factor_line = f"场内基金技术因子显示 `{factor_trend}`"
        factor_momentum = str(fund_factor_snapshot.get("momentum_label", "")).strip()
        factor_date = str(fund_factor_snapshot.get("latest_date", "") or fund_factor_snapshot.get("trade_date", "")).strip()
        if factor_momentum:
            factor_line += f" / `{factor_momentum}`"
        if factor_date:
            factor_line += f"，口径日期 `{factor_date}`。"
        else:
            factor_line += "。"
        reasons.append(factor_line)
    return reasons[:3]


def _etf_fusion_reason_line(analysis: Mapping[str, Any]) -> str:
    asset_type = str(analysis.get("asset_type", "")).strip()
    if asset_type not in {"cn_etf", "cn_fund"}:
        return ""

    metadata = dict(analysis.get("metadata") or {})
    fund_profile = dict(analysis.get("fund_profile") or {})
    etf_snapshot = dict(fund_profile.get("etf_snapshot") or {})
    index_name = str(
        metadata.get("index_framework_label")
        or metadata.get("tracked_index_name")
        or metadata.get("benchmark_name")
        or metadata.get("index_name")
        or etf_snapshot.get("index_name")
        or analysis.get("benchmark_name")
        or ""
    ).strip()
    if not index_name:
        return ""

    try:
        share_value = float(etf_snapshot.get("etf_share_change", metadata.get("etf_share_change")))
    except (TypeError, ValueError):
        share_value = None
    try:
        share_pct = float(etf_snapshot.get("etf_share_change_pct", metadata.get("etf_share_change_pct")))
    except (TypeError, ValueError):
        share_pct = None
    share_as_of = str(etf_snapshot.get("share_as_of", metadata.get("share_as_of", ""))).strip()
    total_share_yi = etf_snapshot.get("total_share_yi")

    share_status = ""
    share_fragment = ""
    if share_value is not None:
        pct_text = f"（{share_pct:+.2f}%）" if share_pct is not None else ""
        if share_value > 0:
            share_status = "positive"
            share_fragment = f"份额最近净创设 `{share_value:+.2f} 亿份`{pct_text}"
        elif share_value < 0:
            share_status = "negative"
            share_fragment = f"份额最近净赎回 `{share_value:+.2f} 亿份`{pct_text}"
        else:
            share_status = "flat"
            share_fragment = "份额最近基本持平"
    else:
        try:
            total_share_yi_value = float(total_share_yi)
        except (TypeError, ValueError):
            total_share_yi_value = None
        if total_share_yi_value is not None and share_as_of:
            share_status = "single_day"
            share_fragment = f"份额端目前只到 `{share_as_of}` 单日快照"

    news_report = dict(analysis.get("news_report") or {})
    linked_news = [
        item
        for item in list(news_report.get("items") or [])
        if str(dict(item).get("link") or "").strip()
    ]
    catalyst = dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {})
    coverage = dict(catalyst.get("coverage") or {})
    intel_status = "thin"
    intel_fragment = "外部情报仍偏薄"
    if linked_news:
        intel_status = "linked"
        intel_fragment = f"外部情报已接上 `{len(linked_news)}` 条可点击线索"
    elif list(news_report.get("items") or []) or list(catalyst.get("theme_news") or []) or list(catalyst.get("evidence") or []):
        intel_status = "tracked"
        intel_fragment = "外部情报已有跟踪，但还不够硬"
    elif coverage.get("degraded") or coverage.get("fallback_applied"):
        intel_fragment = "外部情报当前仍偏薄"

    if share_status == "positive" and intel_status == "linked":
        return f"这次先看它，不只是指数壳：`{index_name}` 的跟踪框架已经清楚，{share_fragment}，{intel_fragment}，产品层和情报层开始同向。"
    if share_status == "positive":
        return f"这次先看它，核心还是产品层确认：`{index_name}` 的跟踪框架已经清楚，{share_fragment}；{intel_fragment}，先不把它直接写成强催化升级。"
    if share_status == "single_day" and intel_status == "linked":
        return f"这次先看它，先按 `{index_name}` 的跟踪框架和外部情报理解；{share_fragment}，当前先不把申赎方向写死。"
    if share_status == "single_day":
        return f"这次先看它，更多是因为 `{index_name}` 的跟踪框架已经清楚；{share_fragment}，{intel_fragment}。"
    if share_status == "negative":
        if intel_status == "linked":
            return f"这次还保留它，主要因为 `{index_name}` 的跟踪框架清楚且 {intel_fragment}；但 {share_fragment}，所以当前先按观察顺位理解。"
        return f"这次还保留它，主要因为 `{index_name}` 的跟踪框架清楚；但 {share_fragment}，{intel_fragment}，当前更像观察顺位而不是强确认。"
    if intel_status in {"linked", "tracked"}:
        return f"这次先看它，主要是 `{index_name}` 的跟踪框架清楚，且 {intel_fragment}。"
    return ""


def _winner_reason_lines(analysis: Dict[str, Any]) -> List[str]:
    narrative = dict(analysis.get("narrative") or {})
    reasons: List[str] = []
    fusion_line = _etf_fusion_reason_line(analysis)
    if fusion_line:
        reasons.append(fusion_line)
    reasons.extend(_etf_structure_reason_lines(analysis))
    reasons.extend(str(item).strip() for item in (narrative.get("positives") or []) if str(item).strip())
    horizon = dict(dict(analysis.get("action") or {}).get("horizon") or {})
    if horizon.get("fit_reason"):
        reasons.append(f"更适合按 `{horizon.get('label', '当前周期')}` 理解：{horizon.get('fit_reason')}")
    dimension_order = [
        ("relative_strength", "相对强弱"),
        ("technical", "技术面"),
        ("fundamental", "基本面"),
        ("catalyst", "催化面"),
        ("risk", "风险特征"),
    ]
    for key, label in dimension_order:
        score = _score_of(analysis, key)
        summary = str(dict(analysis.get("dimensions", {}).get(key) or {}).get("summary", "")).strip()
        if summary:
            reasons.append(f"{label} `{int(score)}` 分：{summary}")
    deduped: List[str] = []
    seen = set()
    for item in reasons:
        if item and item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped[:4]


def _alternative_cautions(analysis: Dict[str, Any]) -> List[str]:
    narrative = dict(analysis.get("narrative") or {})
    cautions = [str(item).strip() for item in (narrative.get("cautions") or []) if str(item).strip()]
    metadata = dict(analysis.get("metadata") or {})
    try:
        share_value = float(metadata.get("etf_share_change"))
    except (TypeError, ValueError):
        share_value = None
    try:
        share_pct = float(metadata.get("etf_share_change_pct"))
    except (TypeError, ValueError):
        share_pct = None
    if share_value is not None and share_value < 0:
        pct_text = f"（{share_pct:+.2f}%）" if share_pct is not None else ""
        cautions.append(f"份额最近净赎回 `{share_value:+.2f} 亿份`{pct_text}，当前主线还没得到场外申购确认。")
    horizon = dict(dict(analysis.get("action") or {}).get("horizon") or {})
    if horizon.get("misfit_reason"):
        cautions.append(f"周期上更像 `{horizon.get('label', '观察期')}`：{horizon.get('misfit_reason')}")
    for key, label in (("technical", "技术面"), ("catalyst", "催化面"), ("risk", "风险特征")):
        score = _score_of(analysis, key)
        summary = str(dict(analysis.get("dimensions", {}).get(key) or {}).get("summary", "")).strip()
        if summary:
            cautions.append(f"{label} `{int(score)}` 分：{summary}")
    deduped: List[str] = []
    seen = set()
    for item in cautions:
        if item and item not in seen:
            deduped.append(item)
            seen.add(item)
    return deduped[:3]


def _positioning_lines(analysis: Dict[str, Any]) -> List[str]:
    action = dict(analysis.get("action") or {})
    return [
        f"首次仓位按 `{action.get('position', '计划仓位的 1/3 - 1/2')}` 执行。",
        f"加仓节奏按 `{action.get('scaling_plan', '确认后再考虑第二笔')}` 执行。",
        f"止损参考按 `{action.get('stop', '重新跌破关键支撑就处理')}` 管理。",
    ]


def _analysis_horizon(analysis: Mapping[str, Any]) -> Dict[str, str]:
    return _pick_horizon_profile(
        dict(analysis.get("action") or {}),
        str(dict(dict(analysis.get("narrative") or {}).get("judgment") or {}).get("state", "")),
    )


def _track_bucket(analysis: Mapping[str, Any]) -> str:
    horizon = _analysis_horizon(analysis)
    code = str(horizon.get("code", "")).strip()
    label = str(horizon.get("label", "")).strip()
    if code in {"short_term", "swing"} or "短线" in label or "波段" in label:
        return "short_term"
    if code in {"position_trade", "long_term_allocation"} or "中线" in label or "长线" in label:
        return "medium_term"
    return ""


def _track_reason(analysis: Mapping[str, Any]) -> str:
    horizon = _analysis_horizon(analysis)
    fit_reason = str(horizon.get("fit_reason", "")).strip()
    if fit_reason:
        return fit_reason
    positives = _winner_reason_lines(dict(analysis))
    return positives[0] if positives else "当前更适合作为跟踪对象，不适合空着不看。"


def _track_payload(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    horizon = _analysis_horizon(analysis)
    action = dict(analysis.get("action") or {})
    return {
        "name": analysis.get("name"),
        "symbol": analysis.get("symbol"),
        "horizon_label": horizon.get("label", "观察期"),
        "trade_state": dict(dict(analysis.get("narrative") or {}).get("judgment") or {}).get("state", action.get("direction", "观察为主")),
        "reason": _track_reason(analysis),
        "reassessment": str(action.get("entry", "")).strip(),
    }


def _recommendation_tracks(ranked: Sequence[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    tracks: Dict[str, Dict[str, Any]] = {}
    used: set[str] = set()

    short_exact = [item for item in ranked if _track_bucket(item) == "short_term"]
    medium_exact = [item for item in ranked if _track_bucket(item) == "medium_term"]

    if short_exact:
        tracks["short_term"] = _track_payload(short_exact[0])
        used.add(str(short_exact[0].get("symbol", "")))
    if medium_exact:
        for item in medium_exact:
            symbol = str(item.get("symbol", ""))
            if symbol in used:
                continue
            tracks["medium_term"] = _track_payload(item)
            used.add(symbol)
            break

    for bucket_name in ("short_term", "medium_term"):
        if bucket_name in tracks:
            continue
        for item in ranked:
            symbol = str(item.get("symbol", ""))
            if symbol in used:
                continue
            tracks[bucket_name] = _track_payload(item)
            used.add(symbol)
            break
    return tracks


def _discovery_mode_label(mode: str) -> str:
    return {
        "tushare_universe": "Tushare 全市场快照",
        "realtime_universe": "实时全市场快照",
        "watchlist_fallback": "watchlist 回退",
        "mixed_pool": "混合池",
    }.get(str(mode), str(mode) or "未标注")


def _selection_context(
    *,
    discovery_mode: str,
    scan_pool: int,
    passed_pool: int,
    theme_filter: str = "",
    blind_spots: Sequence[str] | None = None,
    coverage: Mapping[str, Any] | None = None,
    model_version: str = "",
    baseline_snapshot_at: str = "",
    is_daily_baseline: bool = False,
    comparison_basis_at: str = "",
    comparison_basis_label: str = "",
    model_version_warning: str = "",
    delivery_tier: Mapping[str, Any] | None = None,
    proxy_contract: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    coverage_payload = dict(coverage or {})
    delivery = dict(delivery_tier or {})
    total = int(coverage_payload.get("total") or passed_pool or 0)
    coverage_lines = []
    if total:
        coverage_lines.append(
            f"结构化事件覆盖 {coverage_payload.get('structured_rate', 0.0) * 100:.0f}%（{int(round(coverage_payload.get('structured_rate', 0.0) * total))}/{total}）"
        )
        coverage_lines.append(
            f"高置信直接新闻覆盖 {coverage_payload.get('direct_news_rate', 0.0) * 100:.0f}%（{int(round(coverage_payload.get('direct_news_rate', 0.0) * total))}/{total}）"
        )
    return {
        "discovery_mode": discovery_mode,
        "discovery_mode_label": _discovery_mode_label(discovery_mode),
        "scan_pool": int(scan_pool),
        "passed_pool": int(passed_pool),
        "theme_filter_label": theme_filter or "未指定",
        "blind_spots": [str(item).strip() for item in (blind_spots or []) if str(item).strip()],
        "coverage_note": coverage_payload.get("note", ""),
        "coverage_lines": coverage_lines,
        "coverage_total": total,
        "model_version": model_version,
        "baseline_snapshot_at": baseline_snapshot_at,
        "is_daily_baseline": bool(is_daily_baseline),
        "comparison_basis_at": comparison_basis_at,
        "comparison_basis_label": comparison_basis_label,
        "model_version_warning": model_version_warning,
        "delivery_tier_code": str(delivery.get("code", "")),
        "delivery_tier_label": str(delivery.get("label", "未标注")),
        "delivery_observe_only": bool(delivery.get("observe_only")),
        "delivery_summary_only": bool(delivery.get("summary_only")),
        "delivery_notes": [str(item).strip() for item in delivery.get("notes", []) if str(item).strip()],
        "proxy_contract": dict(proxy_contract or {}),
    }


def _detail_output_path(generated_at: str, theme: str) -> Path:
    date_str = generated_at[:10]
    base = resolve_project_path("reports/etf_picks/internal")
    if theme:
        return base / f"etf_pick_{theme}_{date_str}_internal_detail.md"
    return base / f"etf_pick_{date_str}_internal_detail.md"


def _client_final_runtime_overrides(
    config: Mapping[str, Any],
    *,
    client_final: bool,
    explicit_config_path: str = "",
) -> tuple[Dict[str, Any], List[str]]:
    if not client_final or explicit_config_path.strip():
        return deepcopy(dict(config or {})), []

    effective = deepcopy(dict(config or {}))
    notes: List[str] = []

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
        notes.append("为保证 ETF `client-final` 可交付，本轮自动跳过跨市场代理与 market monitor 慢链。")

    opportunity = dict(effective.get("opportunity") or {})
    current_workers = int(opportunity.get("analysis_workers", 4) or 4)
    if current_workers > 2:
        opportunity["analysis_workers"] = 2
        notes.append("本轮 `client-final` 已自动收窄 ETF 分析并发，优先保证正式稿稳定落盘。")
    current_candidates = int(opportunity.get("max_scan_candidates", 30) or 30)
    if current_candidates > 12:
        opportunity["max_scan_candidates"] = 12
        notes.append("本轮 `client-final` 已自动收窄 ETF 候选池，优先分析更接近正式交付的高流动性样本。")
    if opportunity:
        effective["opportunity"] = opportunity

    if effective.get("skip_fund_profile") is not False:
        effective["skip_fund_profile"] = False
        notes.append("本轮 `client-final` 已保留 ETF 专用基金画像链，正式稿会显式带出跟踪指数、份额规模和持仓画像。")

    if not bool(effective.get("news_topic_search_enabled", False)):
        effective["news_topic_search_enabled"] = True
        notes.append("本轮 `client-final` 保留受控 ETF 主题情报回填，优先补可点击外部事件流，不再只拿盘面句顶替新闻位。")
    current_news_feeds = str(effective.get("news_feeds_file", "") or "").strip()
    if current_news_feeds != "config/news_feeds.briefing_light.yaml":
        effective["news_feeds_file"] = "config/news_feeds.briefing_light.yaml"
        notes.append("本轮 `client-final` 已自动切到轻量非空新闻源配置，优先保留少量可链接情报，不再把 ETF 正式稿新闻完全关空。")
    if str(effective.get("etf_fund_profile_mode", "") or "").strip().lower() != "light":
        effective["etf_fund_profile_mode"] = "light"
        notes.append("本轮 `client-final` 已切到 ETF 轻量候选画像，先用 ETF 专用结构链做排序，再只对入围样本补全完整画像。")
    effective.setdefault("etf_news_backfill_timeout_seconds", 12)
    effective.setdefault("news_topic_query_cap", 4)

    return effective, notes


def _etf_discovery_runtime_overrides(config: Mapping[str, Any]) -> tuple[Dict[str, Any], List[str]]:
    effective = deepcopy(dict(config or {}))
    notes: List[str] = []

    if effective.get("skip_fund_profile") is not False:
        effective["skip_fund_profile"] = False
        notes.append("本轮 ETF discovery 已恢复轻量基金画像链，避免预筛阶段丢失跟踪指数代码。")

    current_profile_mode = str(effective.get("etf_fund_profile_mode", "") or "").strip().lower()
    if not current_profile_mode:
        effective["etf_fund_profile_mode"] = "light"
        notes.append("本轮 ETF discovery 默认启用 `light` 画像，先拿指数代码/跟踪框架，再对入围样本补全 full 画像。")

    if bool(effective.get("news_topic_search_enabled", False)):
        effective["news_topic_search_enabled"] = False
        notes.append("本轮 ETF discovery 先按结构链排序，不在全候选阶段逐只跑主题扩搜；入围样本仍会在成稿阶段补外部情报。")

    effective.setdefault("etf_full_reanalysis_limit", 1)

    return effective, notes


def _is_correlation_only_exclusion(analysis: Mapping[str, Any]) -> bool:
    reasons = [str(item).strip() for item in list(analysis.get("exclusion_reasons") or []) if str(item).strip()]
    return bool(reasons) and all("相关性过高" in item for item in reasons)


def _watchlist_fallback_payload(
    config: Mapping[str, Any],
    *,
    top_n: int,
    theme_filter: str,
) -> Dict[str, Any]:
    lowered_filter = str(theme_filter or "").strip().lower()
    pool = [
        item
        for item in load_watchlist()
        if str(item.get("asset_type", "")).strip() == "cn_etf"
        and (
            not lowered_filter
            or lowered_filter in str(item.get("name", "")).lower()
            or lowered_filter in str(item.get("sector", "")).lower()
        )
    ]
    context = build_market_context(config, relevant_asset_types=["cn_etf", "futures"])
    coverage_analyses: List[Dict[str, Any]] = []
    analyses: List[Dict[str, Any]] = []
    blind_spots = ["全市场 ETF 快照没有形成可交付候选，已回退到 ETF watchlist。"]
    passed = 0
    analysis_workers = max(1, min(int(dict(dict(config).get("opportunity") or {}).get("analysis_workers", 4) or 4), len(pool) or 1, 6))
    base_context = dict(context)
    if analysis_workers > 1 and len(pool) > 1:
        with ThreadPoolExecutor(max_workers=analysis_workers) as executor:
            future_map = {
                executor.submit(
                    analyze_opportunity,
                    str(item["symbol"]),
                    str(item.get("asset_type", "cn_etf")),
                    config,
                    context={**base_context, "runtime_caches": {}},
                    metadata_override={
                        "name": str(item.get("name", item["symbol"])),
                        "sector": str(item.get("sector", "综合")),
                        "chain_nodes": list(item.get("chain_nodes") or []),
                        "region": str(item.get("region", "CN")),
                        "in_watchlist": True,
                    },
                ): item
                for item in pool
            }
            for future in as_completed(future_map):
                item = future_map[future]
                try:
                    analysis = future.result()
                except Exception as exc:
                    blind_spots.append(_client_safe_issue(f"{item['symbol']} ({item.get('name', item['symbol'])}) 扫描失败", exc))
                    continue
                if analysis["excluded"]:
                    if not _is_correlation_only_exclusion(analysis):
                        continue
                    passed += 1
                    coverage_analyses.append(analysis)
                    continue
                passed += 1
                coverage_analyses.append(analysis)
                if analysis["rating"]["rank"] > 0:
                    analyses.append(analysis)
    else:
        for item in pool:
            try:
                analysis = analyze_opportunity(
                    str(item["symbol"]),
                    str(item.get("asset_type", "cn_etf")),
                    config,
                    context=context,
                    metadata_override={
                        "name": str(item.get("name", item["symbol"])),
                        "sector": str(item.get("sector", "综合")),
                        "chain_nodes": list(item.get("chain_nodes") or []),
                        "region": str(item.get("region", "CN")),
                        "in_watchlist": True,
                    },
                )
            except Exception as exc:
                blind_spots.append(_client_safe_issue(f"{item['symbol']} ({item.get('name', item['symbol'])}) 扫描失败", exc))
                continue
            if analysis["excluded"]:
                if not _is_correlation_only_exclusion(analysis):
                    continue
                passed += 1
                coverage_analyses.append(analysis)
                continue
            passed += 1
            coverage_analyses.append(analysis)
            if analysis["rating"]["rank"] > 0:
                analyses.append(analysis)
    if coverage_analyses and not analyses:
        if not any("主题内候选彼此高相关" in item for item in blind_spots):
            blind_spots.append("主题内候选彼此高相关，本轮保留观察稿而不直接输出正式推荐。")
    analyses.sort(key=_rank_key, reverse=True)
    return {
        "generated_at": str(analyses[0].get("generated_at", "")) if analyses else "",
        "scan_pool": len(pool),
        "passed_pool": passed,
        "top": analyses[:top_n],
        "blind_spots": blind_spots,
        "discovery_mode": "watchlist_fallback",
        "data_coverage": summarize_pick_coverage(coverage_analyses),
        "coverage_analyses": coverage_analyses,
    }


def _candidate_summary_rows(analyses: Sequence[Dict[str, Any]]) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in analyses:
        rating = dict(item.get("rating") or {})
        narrative = dict(item.get("narrative") or {})
        horizon = dict(dict(item.get("action") or {}).get("horizon") or {})
        rows.append(
            [
                f"{item.get('name', '—')} ({item.get('symbol', '—')})",
                f"{rating.get('stars', '—')} {rating.get('label', '未评级')}",
                f"{_rank_score(item):.1f}",
                str(dict(narrative.get('judgment') or {}).get("state", "观察为主")),
                str(horizon.get("label", dict(item.get("action") or {}).get("timeframe", "观察期"))).replace("(", "（").replace(")", "）"),
            ]
        )
    return rows


def _detail_markdown(
    analyses: Sequence[Dict[str, Any]],
    winner_symbol: str,
    *,
    selection_context: Mapping[str, Any] | None = None,
) -> str:
    ranked = sorted(analyses, key=_rank_key, reverse=True)
    winner = next((item for item in ranked if str(item.get("symbol", "")) == winner_symbol), ranked[0])
    alternatives = [item for item in ranked if str(item.get("symbol", "")) != str(winner_symbol)]
    generated_at = str(winner.get("generated_at", ""))[:10]
    selection = dict(selection_context or {})
    observe_only = bool(selection.get("delivery_observe_only"))
    heading = "今日ETF观察内部详细稿" if observe_only else "今日ETF推荐内部详细稿"
    winner_label = "观察优先对象" if observe_only else "中选标的"
    lines = [
        f"# {heading} | {generated_at}",
        "",
        f"- 交付等级: `{selection.get('delivery_tier_label', '未标注')}`",
        f"- 发现方式: `{selection.get('discovery_mode_label', '未标注')}`",
        f"- 初筛池: `{selection.get('scan_pool', len(analyses))}`",
        f"- 完整分析: `{selection.get('passed_pool', len(analyses))}`",
        f"- 主题过滤: `{selection.get('theme_filter_label', '未指定')}`",
    ]
    if selection.get("model_version"):
        lines.append(f"- 模型版本: `{selection.get('model_version')}`")
    if selection.get("baseline_snapshot_at"):
        lines.append(f"- 当日基准版: `{selection.get('baseline_snapshot_at')}`")
    if selection.get("comparison_basis_at"):
        lines.append(f"- 分数变动对比基准: `{selection.get('comparison_basis_label', '对比基准')} {selection.get('comparison_basis_at')}`")
    lines.extend(["", "## 数据完整度", ""])
    for item in selection.get("delivery_notes", [])[:4]:
        lines.append(f"- {item}")
    if selection.get("coverage_note"):
        lines.append(f"- {selection.get('coverage_note')}")
    for item in selection.get("coverage_lines", [])[:2]:
        lines.append(f"- {item}")
    if selection.get("model_version_warning"):
        lines.append(f"- 口径提示: {selection.get('model_version_warning')}")
    if selection.get("blind_spots"):
        for item in selection.get("blind_spots", [])[:4]:
            lines.append(f"- {item}")
    lines.extend(["", "## 候选池摘要", ""])
    lines.extend(_table(["标的", "评级", "排序分", "交易状态", "周期"], _candidate_summary_rows(ranked[:5])))
    lines.extend(
        [
            "",
            "## 中选说明",
            "",
            f"- {winner_label}：`{winner.get('name', '—')} ({winner.get('symbol', '—')})`。",
            f"- 选择依据：当前排在候选首位，且客户稿引用的维度分数与动作边界将直接对齐这份详细稿。",
        ]
    )
    if winner.get("score_changes"):
        lines.extend(["", "## 相对基准版的变化", ""])
        for item in winner.get("score_changes", [])[:4]:
            lines.append(f"- `{item.get('label', '维度')}` `{item.get('previous', '—')}` -> `{item.get('current', '—')}`：{item.get('reason', '')}")
    taxonomy = taxonomy_from_analysis(winner)
    lines.extend(["", "## 标准化分类", ""])
    lines.extend(_table(["维度", "结果"], taxonomy_rows(taxonomy)))
    lines.extend(["", f"- {taxonomy.get('summary', '当前分类只作为产品标签，不替代净值、持仓和交易判断。')}"])
    if alternatives:
        lines.extend(["", "## 未中选候选", ""])
        for item in alternatives[:2]:
            lines.append(f"- `{item.get('name', '—')} ({item.get('symbol', '—')})` 保留观察，但当前排序落后于中选标的。")
    if selection.get("blind_spots"):
        lines.extend(["", "## 数据盲区与降级说明", ""])
        for item in selection.get("blind_spots", [])[:5]:
            text = str(item).strip()
            if text:
                lines.append(f"- {text}")
    lines.extend(["", "## 详细分析", ""])
    winner_payload = dict(winner)
    winner_payload["delivery_observe_only"] = observe_only
    winner_payload["delivery_notes"] = list(selection.get("delivery_notes", []) or [])
    lines.append(OpportunityReportRenderer().render_scan(winner_payload).rstrip())
    return "\n".join(lines).rstrip() + "\n"


def _payload_from_analyses(
    analyses: Sequence[Dict[str, Any]],
    selection_context: Dict[str, Any] | None = None,
    *,
    regime: Mapping[str, Any] | None = None,
    day_theme: Mapping[str, Any] | None = None,
    config: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    if not analyses:
        raise ValueError("No ETF analyses available")
    ranked = sorted(analyses, key=_rank_key, reverse=True)
    winner = ranked[0]
    alternatives = ranked[1:3]
    recommendation_tracks = _recommendation_tracks(ranked)
    catalyst = dict(dict(winner.get("dimensions", {}).get("catalyst") or {}))
    evidence = list(catalyst.get("evidence") or [])
    theme_news = list(catalyst.get("theme_news") or [])
    news_report = dict(winner.get("news_report") or {})
    if config:
        news_report = _backfill_etf_news_report(winner, config=config)
    market_event_rows = _market_event_rows(winner)
    share_snapshot_notes = _etf_share_snapshot_notes(ranked)
    if not evidence and not theme_news and not list(news_report.get("items") or []) and not market_event_rows:
        coverage = dict(dict(winner.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {})
        summary = "当前可前置的一手情报有限，判断更多参考结构化事件和行业线索。"
        if coverage.get("fallback_applied"):
            summary = "当前实时情报覆盖不足，本次催化分已按最近一次有效信号做衰减回退，不把临时缺数误读成利空。"
        evidence = [{"title": summary, "source": "覆盖率摘要"}]
    winner_for_delivery = dict(winner)
    winner_for_delivery["news_report"] = news_report
    winner_for_delivery["market_event_rows"] = market_event_rows
    selection_context_payload = dict(selection_context or {})
    if share_snapshot_notes:
        merged_notes: List[str] = []
        for note in [*list(selection_context_payload.get("delivery_notes") or []), *share_snapshot_notes]:
            text = str(note).strip()
            if text and text not in merged_notes:
                merged_notes.append(text)
        selection_context_payload["delivery_notes"] = merged_notes
    return {
        "generated_at": str(winner.get("generated_at", "")),
        "selection_context": selection_context_payload,
        "regime": dict(regime or {}),
        "day_theme": dict(day_theme or {}),
        "recommendation_tracks": recommendation_tracks,
        "winner": {
            "name": winner.get("name"),
            "symbol": winner.get("symbol"),
            "asset_type": winner.get("asset_type"),
            "generated_at": winner.get("generated_at"),
            "strategy_background_confidence": dict(winner.get("strategy_background_confidence") or {}),
            "portfolio_overlap_summary": dict(winner.get("portfolio_overlap_summary") or {}),
            "visuals": dict(winner.get("visuals") or {}),
            "reference_price": float(dict(winner.get("metrics") or {}).get("last_close") or 0.0),
            "trade_state": dict(winner.get("narrative") or {}).get("judgment", {}).get("state", "持有优于追高"),
            "positives": _winner_reason_lines(winner_for_delivery),
            "dimension_rows": _dimension_rows(winner_for_delivery),
            "dimensions": dict(winner.get("dimensions") or {}),
            "action": dict(winner.get("action") or {}),
            "provenance": dict(winner.get("provenance") or {}),
            "intraday": dict(winner.get("intraday") or {}),
            "metadata": dict(winner.get("metadata") or {}),
            "history": winner.get("history"),
            "benchmark_name": winner.get("benchmark_name"),
            "benchmark_symbol": winner.get("benchmark_symbol"),
            "positioning_lines": _positioning_lines(winner),
            "evidence": evidence,
            "news_report": news_report,
            "market_event_rows": market_event_rows,
            "narrative": {"playbook": dict(dict(winner.get("narrative") or {}).get("playbook") or {})},
            "fund_sections": _fund_profile_sections(winner_for_delivery),
            "taxonomy_rows": taxonomy_rows(taxonomy_from_analysis(winner)),
            "taxonomy_summary": str(taxonomy_from_analysis(winner).get("summary", "")),
            "score_changes": list(winner.get("score_changes") or []),
            "comparison_basis_label": str(winner.get("comparison_basis_label", "")),
            "comparison_snapshot_at": str(winner.get("comparison_snapshot_at", "")),
            "proxy_signals": dict(winner.get("proxy_signals") or {}),
        },
        "alternatives": [
            {
                "name": item.get("name"),
                "symbol": item.get("symbol"),
                "cautions": _alternative_cautions(item),
            }
            for item in alternatives
        ],
        "notes": [str(item).strip() for item in (dict(selection_context or {}).get("blind_spots") or []) if str(item).strip()],
    }


def _select_pick_analyses(payload: Mapping[str, Any], *, top_n: int) -> List[Dict[str, Any]]:
    ranked = [dict(item) for item in list(payload.get("top") or []) if isinstance(item, Mapping)]
    if ranked:
        ranked.sort(key=_rank_key, reverse=True)
        return ranked[:top_n]

    coverage_rows = [dict(item) for item in list(payload.get("coverage_analyses") or []) if isinstance(item, Mapping)]
    if not coverage_rows:
        return []

    observation_rows = [
        item
        for item in coverage_rows
        if (
            int(dict(item.get("rating") or {}).get("rank", 0) or 0) >= 0
            and (
                int(dict(dict(item.get("dimensions") or {}).get("fundamental") or {}).get("score", 0) or 0) >= 60
                or int(dict(dict(item.get("dimensions") or {}).get("catalyst") or {}).get("score", 0) or 0) >= 50
                or int(dict(dict(item.get("dimensions") or {}).get("risk") or {}).get("score", 0) or 0) >= 70
                or "观察" in str(dict(dict(item.get("narrative") or {}).get("judgment") or {}).get("state", ""))
                or "持有优于追高" in str(dict(dict(item.get("narrative") or {}).get("judgment") or {}).get("state", ""))
            )
        )
    ]
    selected = observation_rows or coverage_rows
    selected.sort(key=_rank_key, reverse=True)
    return selected[:top_n]


def _promote_observation_candidates(
    payload: Mapping[str, Any],
    *,
    top_n: int,
    reason: str,
) -> Dict[str, Any]:
    selected = _select_pick_analyses(payload, top_n=top_n)
    if not selected:
        return dict(payload)
    promoted = dict(payload)
    promoted["top"] = selected
    blind_spots = [str(item).strip() for item in list(promoted.get("blind_spots") or []) if str(item).strip()]
    if reason not in blind_spots:
        blind_spots.append(reason)
    promoted["blind_spots"] = blind_spots
    return promoted


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("etf_pick")
    setup_logger("ERROR")
    base_config = load_config(args.config or None)
    config, runtime_override_notes = _client_final_runtime_overrides(
        base_config,
        client_final=bool(args.client_final),
        explicit_config_path=args.config,
    )
    config, discovery_override_notes = _etf_discovery_runtime_overrides(config)
    runtime_override_notes = [*runtime_override_notes, *[note for note in discovery_override_notes if note not in runtime_override_notes]]
    try:
        payload = discover_opportunities(config, top_n=max(args.top, 5), theme_filter=args.theme.strip())
        payload = _promote_observation_candidates(
            payload,
            top_n=max(args.top, 5),
            reason="全市场 ETF 扫描未形成正向入围，本次改按观察级候选继续排序，不再直接丢掉已有覆盖样本。",
        )
        if not list(payload.get("top") or []):
            payload = _watchlist_fallback_payload(
                config,
                top_n=max(args.top, 5),
                theme_filter=args.theme.strip(),
            )
            payload = _promote_observation_candidates(
                payload,
                top_n=max(args.top, 5),
                reason="watchlist 回退样本未形成正向入围，本次改按观察级候选继续排序。",
            )
        payload = enrich_pick_payload_with_score_history(
            payload,
            scope=f"theme:{args.theme.strip() or '*'}",
            snapshot_path=SNAPSHOT_PATH,
            model_version=MODEL_VERSION,
            model_changelog=MODEL_CHANGELOG,
            rank_key=_rank_key,
        )
        if runtime_override_notes:
            blind_spots = [str(item).strip() for item in list(payload.get("blind_spots") or []) if str(item).strip()]
            merged_notes: List[str] = []
            for note in runtime_override_notes:
                if note and note not in merged_notes:
                    merged_notes.append(note)
            for item in blind_spots:
                if item and item not in merged_notes:
                    merged_notes.append(item)
            payload["blind_spots"] = merged_notes
        payload["top"] = _attach_strategy_background_confidence(payload.get("top") or [])
        payload["coverage_analyses"] = _attach_strategy_background_confidence(payload.get("coverage_analyses") or [])
        payload["watch_positive"] = _attach_strategy_background_confidence(payload.get("watch_positive") or [])
        payload["top"] = attach_portfolio_overlap_summaries(payload.get("top") or [], config)
        payload["coverage_analyses"] = attach_portfolio_overlap_summaries(payload.get("coverage_analyses") or [], config)
        payload["watch_positive"] = attach_portfolio_overlap_summaries(payload.get("watch_positive") or [], config)
        analyses = _select_pick_analyses(payload, top_n=max(args.top, 5))
        if not analyses:
            raise SystemExit("当前 ETF 推荐池没有可用候选，请稍后重试或放宽主题过滤。")
        analyses = _hydrate_selected_etf_profiles(
            analyses,
            config=config,
            limit=max(1, min(args.top, 2)),
            context=payload.get("runtime_context"),
        )
        attach_visuals_to_analyses(analyses[:3])
        delivery_tier = grade_pick_delivery(
            report_type="etf_pick",
            discovery_mode=str(payload.get("discovery_mode", "")),
            coverage=payload.get("pick_coverage") or payload.get("data_coverage") or {},
            scan_pool=int(payload.get("scan_pool") or 0),
            passed_pool=int(payload.get("passed_pool") or 0),
            winner=analyses[0] if analyses else None,
        )
        selection_context = _selection_context(
            discovery_mode=str(payload.get("discovery_mode", "")),
            scan_pool=int(payload.get("scan_pool") or 0),
            passed_pool=int(payload.get("passed_pool") or 0),
            theme_filter=args.theme.strip(),
            blind_spots=payload.get("blind_spots") or [],
            coverage=payload.get("pick_coverage") or payload.get("data_coverage") or summarize_pick_coverage(analyses),
            model_version=str(payload.get("model_version", "")),
            baseline_snapshot_at=str(payload.get("baseline_snapshot_at", "")),
            is_daily_baseline=bool(payload.get("is_daily_baseline")),
            comparison_basis_at=str(payload.get("comparison_basis_at", "")),
            comparison_basis_label=str(payload.get("comparison_basis_label", "")),
            model_version_warning=str(payload.get("model_version_warning", "")),
            delivery_tier=delivery_tier,
            proxy_contract=payload.get("proxy_contract") or {},
        )
        client_payload = _payload_from_analyses(
            analyses,
            selection_context=selection_context,
            regime=payload.get("regime") or {},
            day_theme=payload.get("day_theme") or {},
            config=config,
        )
        delivery_tier = grade_pick_delivery(
            report_type="etf_pick",
            discovery_mode=str(payload.get("discovery_mode", "")),
            coverage=payload.get("pick_coverage") or payload.get("data_coverage") or {},
            scan_pool=int(payload.get("scan_pool") or 0),
            passed_pool=int(payload.get("passed_pool") or 0),
            winner=dict(client_payload.get("winner") or {}),
        )
        selection_context = _selection_context(
            discovery_mode=str(payload.get("discovery_mode", "")),
            scan_pool=int(payload.get("scan_pool") or 0),
            passed_pool=int(payload.get("passed_pool") or 0),
            theme_filter=args.theme.strip(),
            blind_spots=payload.get("blind_spots") or [],
            coverage=payload.get("pick_coverage") or payload.get("data_coverage") or summarize_pick_coverage(analyses),
            model_version=str(payload.get("model_version", "")),
            baseline_snapshot_at=str(payload.get("baseline_snapshot_at", "")),
            is_daily_baseline=bool(payload.get("is_daily_baseline")),
            comparison_basis_at=str(payload.get("comparison_basis_at", "")),
            comparison_basis_label=str(payload.get("comparison_basis_label", "")),
            model_version_warning=str(payload.get("model_version_warning", "")),
            delivery_tier=delivery_tier,
            proxy_contract=payload.get("proxy_contract") or {},
        )
        date_str = str(client_payload.get("generated_at", ""))[:10]
        theme = args.theme.strip().replace("/", "_").replace(" ", "_")
        detail_path = _detail_output_path(str(client_payload.get("generated_at", "")), theme)
        if args.client_final:
            catalyst_review_path = internal_sidecar_path(detail_path, "catalyst_web_review.md")
            review_lookup = load_catalyst_web_review(catalyst_review_path)
            if review_lookup:
                analyses = [attach_catalyst_web_review_to_analysis(item, review_lookup) for item in analyses]
                client_payload = _payload_from_analyses(
                    analyses,
                    selection_context=selection_context,
                    regime=payload.get("regime") or {},
                    day_theme=payload.get("day_theme") or {},
                    config=config,
                )
        client_payload["selection_context"] = selection_context
        markdown = ClientReportRenderer().render_etf_pick(client_payload)
        if not args.client_final:
            print(markdown)
            return

        filename = f"etf_pick_{theme}_{date_str}_final.md" if theme else f"etf_pick_{date_str}_final.md"
        markdown_path = resolve_project_path(f"reports/etf_picks/final/{filename}")
        detail_markdown = _detail_markdown(
            analyses,
            str(dict(client_payload.get("winner") or {}).get("symbol", "")),
            selection_context=selection_context,
        )
        factor_contract = summarize_factor_contracts_from_analyses(list(payload.get("coverage_analyses") or analyses), sample_limit=16)
        catalyst_review_path = internal_sidecar_path(detail_path, "catalyst_web_review.md")
        editor_packet = build_etf_pick_editor_packet(client_payload)
        editor_prompt = render_financial_editor_prompt(editor_packet)
        catalyst_packet = build_catalyst_web_review_packet(
            report_type="etf_pick",
            subject=f"etf_pick {date_str}",
            generated_at=str(client_payload.get("generated_at", "")),
            analyses=list(payload.get("coverage_analyses") or analyses),
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
            report_type="etf_pick",
            client_markdown=markdown,
            markdown_path=markdown_path,
            detail_markdown=detail_markdown,
            detail_path=detail_path,
            extra_manifest={
                "theme_filter": args.theme.strip(),
                "winner": dict(client_payload.get("winner") or {}).get("symbol", ""),
                "scan_pool": int(payload.get("scan_pool") or 0),
                "passed_pool": int(payload.get("passed_pool") or 0),
                "discovery_mode": str(payload.get("discovery_mode", "")),
                "delivery_tier": dict(delivery_tier),
            "data_coverage": dict(payload.get("pick_coverage") or {}),
            "factor_contract": factor_contract,
            "proxy_contract": dict(payload.get("proxy_contract") or {}),
            "theme_playbook_contract": summarize_theme_playbook_contract(editor_packet.get("theme_playbook") or {}),
            "event_digest_contract": summarize_event_digest_contract(editor_packet.get("event_digest") or {}),
            "what_changed_contract": summarize_what_changed_contract(editor_packet.get("what_changed") or {}),
        },
        release_checker=lambda markdown_text, source_text: check_generic_client_report(
            markdown_text,
            "etf_pick",
            source_text=source_text,
            editor_theme_playbook=editor_packet.get("theme_playbook") or {},
            editor_prompt_text=editor_prompt,
            event_digest_contract=editor_packet.get("event_digest") or {},
            what_changed_contract=editor_packet.get("what_changed") or {},
        ),
            text_sidecars=text_sidecars,
            json_sidecars=json_sidecars,
        )
        print(markdown)
        for index, line in enumerate(exported_bundle_lines(bundle)):
            print(f"\n{line}" if index == 0 else line)
    finally:
        close_yfinance_runtime_caches()


if __name__ == "__main__":
    main()
