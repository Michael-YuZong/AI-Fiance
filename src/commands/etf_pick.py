"""Daily ETF recommendation command."""

from __future__ import annotations

import argparse
from concurrent.futures import ThreadPoolExecutor, as_completed
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from src.commands.pick_history import enrich_pick_payload_with_score_history, grade_pick_delivery, summarize_pick_coverage
from src.commands.pick_visuals import attach_visuals_to_analyses
from src.commands.report_guard import ReportGuardError, ensure_report_task_registered, export_reviewed_markdown_bundle, exported_bundle_lines
from src.commands.release_check import check_generic_client_report
from src.output import ClientReportRenderer, OpportunityReportRenderer
from src.output.client_report import _fund_profile_sections, _pick_horizon_profile
from src.processors.factor_meta import summarize_factor_contracts_from_analyses
from src.processors.opportunity_engine import _client_safe_issue, analyze_opportunity, build_market_context, discover_opportunities
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
        display = "—" if score is None else f"{score}/{max_score}"
        reason = str(dimension.get("summary", "")).strip() or str(dimension.get("core_signal", "")).strip()
        rows.append([str(dimension.get("display_name", label)), display, reason])
    return rows


def _rank_score(analysis: Dict[str, Any]) -> float:
    return (
        _score_of(analysis, "technical") * 0.22
        + _score_of(analysis, "fundamental") * 0.18
        + _score_of(analysis, "catalyst") * 0.18
        + _score_of(analysis, "relative_strength") * 0.22
        + _score_of(analysis, "risk") * 0.12
        + _score_of(analysis, "macro") * 0.08
    )


def _rank_key(analysis: Mapping[str, Any]) -> tuple[float, float, float, float]:
    return (
        float(int(analysis.get("rating", {}).get("rank", 0) or 0)),
        _rank_score(dict(analysis)),
        _score_of(dict(analysis), "relative_strength"),
        _score_of(dict(analysis), "catalyst"),
    )


def _winner_reason_lines(analysis: Dict[str, Any]) -> List[str]:
    narrative = dict(analysis.get("narrative") or {})
    reasons: List[str] = []
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
                continue
            passed += 1
            coverage_analyses.append(analysis)
            if analysis["rating"]["rank"] > 0:
                analyses.append(analysis)
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
    lines = [
        f"# 今日ETF推荐内部详细稿 | {generated_at}",
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
            f"- 中选标的：`{winner.get('name', '—')} ({winner.get('symbol', '—')})`。",
            f"- 中选依据：当前候选里评级与综合排序分最优，且客户稿引用的维度分数将直接对齐这份详细稿。",
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
    lines.extend(["", "## 中选标的详细分析", ""])
    lines.append(OpportunityReportRenderer().render_scan(dict(winner)).rstrip())
    return "\n".join(lines).rstrip() + "\n"


def _payload_from_analyses(analyses: Sequence[Dict[str, Any]], selection_context: Dict[str, Any] | None = None) -> Dict[str, Any]:
    if not analyses:
        raise ValueError("No ETF analyses available")
    ranked = sorted(analyses, key=_rank_key, reverse=True)
    winner = ranked[0]
    alternatives = ranked[1:3]
    recommendation_tracks = _recommendation_tracks(ranked)
    evidence = list(dict(winner.get("dimensions", {}).get("catalyst") or {}).get("evidence") or [])
    if not evidence:
        coverage = dict(dict(winner.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {})
        summary = "当前没有抓到高置信直连证据，催化判断更多依赖结构化事件或行业映射。"
        if coverage.get("fallback_applied"):
            summary = "当前实时新闻覆盖不足，本次催化分已按最近一次有效信号做衰减回退，不把临时缺数误当成利空。"
        evidence = [{"title": summary, "source": "内部覆盖率摘要"}]
    return {
        "generated_at": str(winner.get("generated_at", "")),
        "selection_context": dict(selection_context or {}),
        "recommendation_tracks": recommendation_tracks,
        "winner": {
            "name": winner.get("name"),
            "symbol": winner.get("symbol"),
            "asset_type": winner.get("asset_type"),
            "visuals": dict(winner.get("visuals") or {}),
            "reference_price": float(dict(winner.get("metrics") or {}).get("last_close") or 0.0),
            "trade_state": dict(winner.get("narrative") or {}).get("judgment", {}).get("state", "持有优于追高"),
            "positives": _winner_reason_lines(winner),
            "dimension_rows": _dimension_rows(winner),
            "action": dict(winner.get("action") or {}),
            "positioning_lines": _positioning_lines(winner),
            "evidence": evidence,
            "narrative": {"playbook": dict(dict(winner.get("narrative") or {}).get("playbook") or {})},
            "fund_sections": _fund_profile_sections(winner),
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


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("etf_pick")
    setup_logger("ERROR")
    config = load_config(args.config or None)
    try:
        payload = discover_opportunities(config, top_n=max(args.top, 5), theme_filter=args.theme.strip())
        if not list(payload.get("top") or []):
            payload = _watchlist_fallback_payload(
                config,
                top_n=max(args.top, 5),
                theme_filter=args.theme.strip(),
            )
        payload = enrich_pick_payload_with_score_history(
            payload,
            scope=f"theme:{args.theme.strip() or '*'}",
            snapshot_path=SNAPSHOT_PATH,
            model_version=MODEL_VERSION,
            model_changelog=MODEL_CHANGELOG,
            rank_key=_rank_key,
        )
        analyses = list(payload.get("top") or [])
        if not analyses:
            raise SystemExit("当前 ETF 推荐池没有可用候选，请稍后重试或放宽主题过滤。")
        attach_visuals_to_analyses(analyses[:3])
        delivery_tier = grade_pick_delivery(
            report_type="etf_pick",
            discovery_mode=str(payload.get("discovery_mode", "")),
            coverage=payload.get("pick_coverage") or payload.get("data_coverage") or {},
            scan_pool=int(payload.get("scan_pool") or 0),
            passed_pool=int(payload.get("passed_pool") or 0),
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
        client_payload = _payload_from_analyses(analyses, selection_context=selection_context)
        markdown = ClientReportRenderer().render_etf_pick(client_payload)
        if not args.client_final:
            print(markdown)
            return

        date_str = str(client_payload.get("generated_at", ""))[:10]
        theme = args.theme.strip().replace("/", "_").replace(" ", "_")
        filename = f"etf_pick_{theme}_{date_str}_final.md" if theme else f"etf_pick_{date_str}_final.md"
        detail_markdown = _detail_markdown(
            analyses,
            str(dict(client_payload.get("winner") or {}).get("symbol", "")),
            selection_context=selection_context,
        )
        factor_contract = summarize_factor_contracts_from_analyses(list(payload.get("coverage_analyses") or analyses), sample_limit=16)
        detail_path = _detail_output_path(str(client_payload.get("generated_at", "")), theme)
        detail_path.parent.mkdir(parents=True, exist_ok=True)
        detail_path.write_text(detail_markdown, encoding="utf-8")
        findings = check_generic_client_report(markdown, "etf_pick", source_text=detail_markdown)
        try:
            bundle = export_reviewed_markdown_bundle(
                report_type="etf_pick",
                markdown_text=markdown,
                markdown_path=resolve_project_path(f"reports/etf_picks/final/{filename}"),
                release_findings=findings,
                extra_manifest={
                    "theme_filter": args.theme.strip(),
                    "winner": dict(client_payload.get("winner") or {}).get("symbol", ""),
                    "detail_source": str(detail_path),
                    "scan_pool": int(payload.get("scan_pool") or 0),
                    "passed_pool": int(payload.get("passed_pool") or 0),
                    "discovery_mode": str(payload.get("discovery_mode", "")),
                    "delivery_tier": dict(delivery_tier),
                    "data_coverage": dict(payload.get("pick_coverage") or {}),
                    "factor_contract": factor_contract,
                    "proxy_contract": dict(payload.get("proxy_contract") or {}),
                },
            )
        except ReportGuardError as exc:
            raise SystemExit(str(exc))
        print(markdown)
        for index, line in enumerate(exported_bundle_lines(bundle)):
            print(f"\n{line}" if index == 0 else line)
    finally:
        close_yfinance_runtime_caches()


if __name__ == "__main__":
    main()
