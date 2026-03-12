"""Stock pick command — scan stock universe and surface top individual stock picks."""

from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from src.commands.report_guard import ReportGuardError, ensure_report_task_registered, export_reviewed_markdown_bundle
from src.output import ClientReportRenderer, OpportunityReportRenderer
from src.processors.opportunity_engine import _rating_from_dimensions, discover_stock_opportunities
from src.utils.config import PROJECT_ROOT, load_config
from src.utils.data import load_json, save_json
from src.utils.logger import setup_logger

SNAPSHOT_PATH = PROJECT_ROOT / "data" / "stock_pick_score_history.json"
FINAL_DIR = PROJECT_ROOT / "reports" / "stock_picks" / "final"
INTERNAL_DIR = PROJECT_ROOT / "reports" / "stock_picks" / "internal"
MODEL_VERSION = "stock-pick-2026-03-11-indicator-sanity-v6"
MODEL_CHANGELOG = [
    "A 股估值口径统一为 `PE_TTM`；动态 PE 不再混入滚动 PE。",
    "个股负面事件窗口扩展为 `30` 日衰减，并补了英文监管/稀释关键词。",
    "同一天的报告默认锁定首个可用输出为 `当日基准版`；后续重跑统一和基准版对比。",
    "风险维度的回撤恢复改为看 `近一年高点后的修复速度/修复比例`，不再把长期未创新高统一打成 `999 日`。",
    "DMI/ADX 改为 `Wilder smoothing` 口径，不再用简单滚动均值；这会影响趋势强度分和技术面摘要。",
    "RSI 改为 Wilder 初始均值口径，KDJ 改为以 `50` 为种子递推，避免和主流行情软件出现系统性偏差。",
    "图表层不再重复自算技术指标，统一复用 `TechnicalAnalyzer` 输出，避免图表和报告口径分叉。",
    "技术面里的 `量比` 文案改为 `量能比`，明确表示这里使用的是日成交量相对 5 日均量。",
    "催化面核心信号优先展示个股直连标题，减少所有股票都显示同一组市场新闻的问题。",
    "HK/US 个股前瞻事件改为优先读取公司级财报日历；未来 `14` 日财报日会进入催化和风险窗口。",
    "交易参数增加硬校验，默认满足 `止损价 < 当前价 < 目标价`，避免把阻力位误标成止损。",
    "HK/US 个股若未命中公司直连新闻，政策/龙头/海外映射催化不再做正向加分，避免把市场级新闻误记成个股催化。",
    "英文股票名不再使用两字符前缀做模糊匹配，避免 `Meta -> Me` 这类误命中。",
    "美股短英文 ticker 改为按单词边界匹配，避免 `SNOW -> snowfall` 这类误命中。",
]
DIMENSION_LABELS = {
    "technical": "技术面",
    "fundamental": "基本面",
    "catalyst": "催化面",
    "relative_strength": "相对强弱",
    "chips": "筹码结构",
    "risk": "风险特征",
    "seasonality": "季节/日历",
    "macro": "宏观敏感度",
}


def _scope_key(market: str, sector_filter: str) -> str:
    return f"{market}:{sector_filter or '*'}"


def _score_value(value: Any) -> Optional[float]:
    if value in (None, "", "—", "缺失", "信息项"):
        return None
    text = str(value).strip()
    if not text:
        return None
    if "/" in text:
        text = text.split("/", 1)[0].strip()
    match = re.search(r"-?\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group(0))


def _factor_snapshot(dimension: Mapping[str, Any]) -> Dict[str, Dict[str, str]]:
    factors: Dict[str, Dict[str, str]] = {}
    for item in dimension.get("factors", []) or []:
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        factors[name] = {
            "display_score": str(item.get("display_score", "")),
            "signal": str(item.get("signal", "")),
        }
    return factors


def _analysis_snapshot(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    dimensions = {
        key: {
            "score": analysis.get("dimensions", {}).get(key, {}).get("score"),
            "core_signal": str(analysis.get("dimensions", {}).get(key, {}).get("core_signal", "")),
            "factors": _factor_snapshot(analysis.get("dimensions", {}).get(key, {})),
        }
        for key in DIMENSION_LABELS
    }
    return {
        "name": str(analysis.get("name", "")),
        "rating_rank": int(analysis.get("rating", {}).get("rank", 0) or 0),
        "dimensions": dimensions,
    }


def _previous_scope_snapshot(scope_history: Mapping[str, Any], current_date: str) -> Dict[str, Any]:
    daily = dict(scope_history.get("daily_baselines") or {})
    candidates = []
    for key, value in daily.items():
        if key == current_date:
            continue
        generated_at = str(dict(value).get("generated_at", ""))
        candidates.append((generated_at or f"{key} 00:00:00", dict(value)))
    if candidates:
        candidates.sort(key=lambda item: item[0], reverse=True)
        return dict(candidates[0][1])
    latest = dict(scope_history.get("latest") or {})
    if _snapshot_date(str(latest.get("generated_at", ""))) != current_date:
        return latest
    return {}


def _coverage_rows(analyses: list[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    markets = {"A股": [], "港股": [], "美股": []}
    for item in analyses:
        markets.setdefault(
            {"cn_stock": "A股", "hk": "港股", "us": "美股"}.get(str(item.get("asset_type", "")), "其他"),
            [],
        ).append(item)

    payload: Dict[str, Dict[str, Any]] = {}
    for market, rows in markets.items():
        if not rows:
            continue
        total = len(rows)
        structured = 0
        direct = 0
        degraded = 0
        for item in rows:
            coverage = dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {})
            if coverage.get("structured_event") or coverage.get("forward_event"):
                structured += 1
            if coverage.get("high_confidence_company_news"):
                direct += 1
            if coverage.get("degraded"):
                degraded += 1
        payload[market] = {
            "total": total,
            "structured_rate": structured / total if total else 0.0,
            "direct_rate": direct / total if total else 0.0,
            "degraded_rate": degraded / total if total else 0.0,
        }
    return payload


def _coverage_summary(analyses: list[Mapping[str, Any]]) -> Dict[str, Any]:
    by_market = _coverage_rows(analyses)
    lines = []
    for market in ("A股", "港股", "美股"):
        row = by_market.get(market)
        if not row:
            continue
        lines.append(
            f"{market} 结构化事件覆盖 {row['structured_rate'] * 100:.0f}% / 高置信公司新闻覆盖 {row['direct_rate'] * 100:.0f}%"
        )
    overall_degraded = any((row.get("degraded_rate", 0.0) or 0.0) > 0.5 for row in by_market.values())
    note = "本轮实时新闻/事件覆盖存在降级，名单更容易偏保守。" if overall_degraded else "本轮新闻/事件覆盖基本正常。"
    return {"by_market": by_market, "lines": lines, "note": note}


def _maybe_apply_catalyst_fallback(
    analysis: Dict[str, Any],
    *,
    previous_items: Mapping[str, Any],
    fallback_allowed: bool,
) -> None:
    if not fallback_allowed:
        return
    symbol = str(analysis.get("symbol", "")).strip()
    if not symbol:
        return
    previous = dict(previous_items.get(symbol) or {})
    previous_catalyst = dict(previous.get("dimensions", {}).get("catalyst") or {})
    prev_score = previous_catalyst.get("score")
    if prev_score is None:
        return

    catalyst_dimension = dict(analysis.get("dimensions", {}).get("catalyst") or {})
    coverage = dict(catalyst_dimension.get("coverage") or {})
    current_score = catalyst_dimension.get("score")
    if current_score is None:
        return
    if coverage.get("high_confidence_company_news") or coverage.get("structured_event") or coverage.get("forward_event"):
        return
    if int(current_score) >= int(prev_score):
        return
    if int(prev_score) < 40 or int(current_score) > 25:
        return

    fallback_score = max(int(current_score), int(round(int(prev_score) * 0.7)))
    if fallback_score <= int(current_score):
        return

    factors = list(catalyst_dimension.get("factors") or [])
    factors.append(
        {
            "name": "历史催化回退",
            "signal": f"实时新闻降级，回退最近一次有效催化快照（前值 {prev_score}/100）",
            "awarded": fallback_score - int(current_score),
            "max": 100,
            "detail": "新闻源降级时不直接把催化打成假阴性；这里仅做衰减回退，不视作新的新增催化。",
            "display_score": f"+{fallback_score - int(current_score)}",
        }
    )
    catalyst_dimension["score"] = fallback_score
    catalyst_dimension["summary"] = "当前实时新闻覆盖不足，已用最近一次有效催化快照做衰减回退；这会提高鲁棒性，但不等于今天出现了新增利好。"
    catalyst_dimension["core_signal"] = str(catalyst_dimension.get("core_signal", "")).strip() or "实时新闻降级，催化按历史有效信号衰减回退"
    catalyst_dimension["factors"] = factors
    coverage["fallback_applied"] = True
    catalyst_dimension["coverage"] = coverage
    analysis["dimensions"]["catalyst"] = catalyst_dimension
    analysis["rating"] = _rating_from_dimensions(analysis["dimensions"], analysis.get("rating", {}).get("warnings", []) or [])


def _rank_key(item: Mapping[str, Any]) -> tuple[float, float, float, float]:
    dimensions = dict(item.get("dimensions") or {})
    total_score = float(
        sum(float(dict(dimension).get("score") or 0) for dimension in dimensions.values())
    )
    return (
        float(int(item.get("rating", {}).get("rank", 0) or 0)),
        total_score,
        float(dict(dimensions.get("relative_strength") or {}).get("score") or 0),
        float(dict(dimensions.get("fundamental") or {}).get("score") or 0),
    )


def _build_snapshot(top: list[Mapping[str, Any]], generated_at: str) -> Dict[str, Any]:
    return {
        "generated_at": generated_at,
        "model_version": MODEL_VERSION,
        "items": {str(item.get("symbol", "")): _analysis_snapshot(item) for item in top if str(item.get("symbol", "")).strip()},
    }


def _snapshot_date(generated_at: str) -> str:
    return str(generated_at or "").split(" ", 1)[0]


def _normalize_scope_history(payload: Mapping[str, Any]) -> Dict[str, Any]:
    history = dict(payload or {})
    if "latest" in history or "daily_baselines" in history:
        return {
            "latest": dict(history.get("latest") or {}),
            "daily_baselines": dict(history.get("daily_baselines") or {}),
        }
    if history.get("items"):
        return {
            "latest": dict(history),
            "daily_baselines": {},
        }
    return {
        "latest": {},
        "daily_baselines": {},
    }


def _format_change(display_value: str) -> str:
    text = str(display_value or "").strip()
    return text or "信息项"


def _factor_change_reason(name: str, previous: Mapping[str, Any], current: Mapping[str, Any]) -> tuple[float, str] | None:
    prev_display = _format_change(str(previous.get("display_score", "")))
    curr_display = _format_change(str(current.get("display_score", "")))
    prev_score = _score_value(prev_display)
    curr_score = _score_value(curr_display)
    if prev_score is None and curr_score is None:
        prev_signal = str(previous.get("signal", "")).strip()
        curr_signal = str(current.get("signal", "")).strip()
        if prev_signal and curr_signal and prev_signal != curr_signal:
            return 0.0, f"{name} 信号从 `{prev_signal}` 变为 `{curr_signal}`"
        return None
    delta = (curr_score or 0.0) - (prev_score or 0.0)
    if abs(delta) < 5:
        return None
    if prev_score is None:
        return delta, f"{name} 从 `{prev_display}` 变为 `{curr_display}`"
    if curr_score is None:
        return delta, f"{name} 从 `{prev_display}` 变为 `{curr_display}`"
    return delta, f"{name} `{prev_display}` -> `{curr_display}`"


def _dimension_change(previous: Mapping[str, Any], current: Mapping[str, Any], dimension_key: str) -> Optional[Dict[str, Any]]:
    prev_dimension = dict(previous.get("dimensions", {}).get(dimension_key) or {})
    curr_dimension = dict(current.get("dimensions", {}).get(dimension_key) or {})
    prev_score = prev_dimension.get("score")
    curr_score = curr_dimension.get("score")
    if prev_score is None or curr_score is None:
        return None
    delta = int(curr_score) - int(prev_score)
    if abs(delta) < 10:
        return None
    previous_factors = dict(prev_dimension.get("factors") or {})
    current_factors = dict(curr_dimension.get("factors") or {})
    factor_changes = []
    for name in sorted(set(previous_factors) | set(current_factors)):
        reason = _factor_change_reason(name, previous_factors.get(name, {}), current_factors.get(name, {}))
        if reason:
            factor_changes.append(reason)
    factor_changes.sort(key=lambda item: abs(item[0]), reverse=True)
    if factor_changes:
        reason_text = "；".join(text for _, text in factor_changes[:2])
    else:
        prev_signal = str(prev_dimension.get("core_signal", "")).strip()
        curr_signal = str(curr_dimension.get("core_signal", "")).strip()
        reason_text = (
            f"核心信号从 `{prev_signal}` 变为 `{curr_signal}`"
            if prev_signal and curr_signal and prev_signal != curr_signal
            else "主因是子项重算或新闻/行情快照更新。"
        )
    return {
        "dimension": dimension_key,
        "label": DIMENSION_LABELS[dimension_key],
        "previous": int(prev_score),
        "current": int(curr_score),
        "delta": delta,
        "reason": reason_text,
    }


def enrich_payload_with_score_history(
    payload: Dict[str, Any],
    market: str,
    sector_filter: str,
    snapshot_path: Path = SNAPSHOT_PATH,
) -> Dict[str, Any]:
    history_store = load_json(snapshot_path, default={}) or {}
    scope = _scope_key(market, sector_filter)
    scope_history = _normalize_scope_history(history_store.get(scope) or {})
    current_snapshot = _build_snapshot(payload.get("top", []) or [], str(payload.get("generated_at", "")))
    current_date = _snapshot_date(str(payload.get("generated_at", "")))
    baseline_scope = dict((scope_history.get("daily_baselines") or {}).get(current_date) or {})
    comparison_scope = baseline_scope

    if not baseline_scope:
        comparison_scope = {}

    comparison_items = dict(comparison_scope.get("items") or {})
    previous_scope = _previous_scope_snapshot(scope_history, current_date)
    previous_items = dict(previous_scope.get("items") or {})
    comparison_basis_label = "当日基准版" if comparison_scope else ""
    comparison_basis_at = str(comparison_scope.get("generated_at", "")) if comparison_scope else ""
    comparison_model_version = str(comparison_scope.get("model_version", "")) if comparison_scope else ""
    model_version_warning = ""
    if comparison_model_version and comparison_model_version != MODEL_VERSION:
        model_version_warning = f"当前 `{MODEL_VERSION}`，基准版 `{comparison_model_version}`，两次运行的规则口径并不完全相同。"

    fallback_allowed = (
        str(dict(payload.get("data_coverage") or {}).get("news_mode", "")) != "live"
        or bool(dict(payload.get("data_coverage") or {}).get("degraded"))
    )
    all_items = list(payload.get("top", []) or [])
    for analysis in all_items:
        _maybe_apply_catalyst_fallback(
            analysis,
            previous_items=previous_items,
            fallback_allowed=fallback_allowed,
        )

    payload["top"] = sorted(all_items, key=_rank_key, reverse=True)[: len(all_items)]
    payload["watch_positive"] = sorted(
        [
            analysis
            for analysis in all_items
            if int(analysis.get("rating", {}).get("rank", 0) or 0) < 3
            and (
                (analysis["dimensions"]["fundamental"].get("score") or 0) >= 60
                or (analysis["dimensions"]["catalyst"].get("score") or 0) >= 50
                or (analysis["dimensions"]["relative_strength"].get("score") or 0) >= 70
                or (analysis["dimensions"]["risk"].get("score") or 0) >= 70
            )
        ],
        key=_rank_key,
        reverse=True,
    )[:6]

    for analysis in payload.get("top", []) or []:
        previous = dict(comparison_items.get(str(analysis.get("symbol", ""))) or {})
        score_changes = []
        if previous:
            current_analysis_snapshot = _analysis_snapshot(analysis)
            for key in DIMENSION_LABELS:
                change = _dimension_change(previous, current_analysis_snapshot, key)
                if change:
                    score_changes.append(change)
        analysis["score_changes"] = score_changes
        analysis["comparison_snapshot_at"] = comparison_basis_at if previous else ""
        analysis["comparison_basis_label"] = comparison_basis_label if previous else ""
        analysis["previous_snapshot_at"] = comparison_basis_at if previous else ""

    payload["model_version"] = MODEL_VERSION
    payload["comparison_basis_label"] = comparison_basis_label
    payload["comparison_basis_at"] = comparison_basis_at
    payload["comparison_model_version"] = comparison_model_version
    payload["previous_snapshot_at"] = comparison_basis_at
    payload["baseline_snapshot_at"] = str(baseline_scope.get("generated_at", "")) if baseline_scope else str(current_snapshot.get("generated_at", ""))
    payload["baseline_model_version"] = str(baseline_scope.get("model_version", "")) if baseline_scope else MODEL_VERSION
    payload["is_daily_baseline"] = not bool(baseline_scope)
    payload["model_version_warning"] = model_version_warning
    payload["model_changelog"] = MODEL_CHANGELOG
    payload["stock_pick_coverage"] = _coverage_summary(all_items)

    daily_baselines = dict(scope_history.get("daily_baselines") or {})
    if current_date and not baseline_scope:
        daily_baselines[current_date] = current_snapshot
    history_store[scope] = {
        "latest": current_snapshot,
        "daily_baselines": daily_baselines,
    }
    save_json(snapshot_path, history_store)
    return payload


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Scan stock universe and surface top individual stock picks.")
    parser.add_argument("--market", default="all", choices=["cn", "hk", "us", "all"], help="Market scope: cn (A-share), hk, us, or all")
    parser.add_argument("--sector", default="", help="Sector filter, e.g. 科技 / 消费 / 医药")
    parser.add_argument("--top", type=int, default=20, help="Number of top picks to show")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")
    return parser


def _internal_detail_stem(market: str, generated_at: str) -> str:
    return f"stock_picks_{market}_{generated_at[:10]}_internal_detail"


def _internal_merged_stem(generated_at: str) -> str:
    return f"stock_picks_{generated_at[:10]}_internal_detail"


def _final_stem(generated_at: str) -> str:
    return f"stock_picks_{generated_at[:10]}_final"


def _market_final_stem(market: str, generated_at: str) -> str:
    return f"stock_picks_{market}_{generated_at[:10]}_final"


def _persist_internal_detail_report(stem: str, markdown: str) -> Path:
    INTERNAL_DIR.mkdir(parents=True, exist_ok=True)
    path = INTERNAL_DIR / f"{stem}.md"
    path.write_text(markdown, encoding="utf-8")
    return path


def _merge_payloads(payloads: Mapping[str, Mapping[str, Any]]) -> Dict[str, Any]:
    merged_top = []
    merged_watch = []
    generated_at = ""
    blind_spots = []
    for market in ("cn", "hk", "us"):
        payload = dict(payloads.get(market) or {})
        if payload and not generated_at:
            generated_at = str(payload.get("generated_at", ""))
        merged_top.extend(payload.get("top", []) or [])
        merged_watch.extend(payload.get("watch_positive", []) or [])
        blind_spots.extend(payload.get("blind_spots", []) or [])
    merged_top = sorted(merged_top, key=_rank_key, reverse=True)
    merged_watch = sorted(merged_watch, key=_rank_key, reverse=True)
    first = dict(next(iter(payloads.values())) or {})
    coverage = _coverage_summary(merged_top)
    return {
        "generated_at": generated_at,
        "top": merged_top,
        "watch_positive": merged_watch,
        "day_theme": dict(first.get("day_theme") or {}),
        "regime": dict(first.get("regime") or {}),
        "stock_pick_coverage": coverage,
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
) -> Dict[str, Any]:
    payload = discover_stock_opportunities(config, top_n=top_n, market=market, sector_filter=sector_filter)
    return enrich_payload_with_score_history(payload, market=market, sector_filter=sector_filter)


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("stock_pick")
    setup_logger("ERROR")
    config = load_config(args.config or None)
    sector_filter = args.sector.strip()

    if not args.client_final:
        payload = discover_stock_opportunities(config, top_n=args.top, market=args.market, sector_filter=sector_filter)
        payload = enrich_payload_with_score_history(payload, market=args.market, sector_filter=sector_filter)
        print(OpportunityReportRenderer().render_stock_picks(payload))
        return

    if args.market == "all":
        market_payloads = {
            market: _run_market(config, market, args.top, sector_filter)
            for market in ("cn", "hk", "us")
        }
        for market, payload in market_payloads.items():
            detailed = OpportunityReportRenderer().render_stock_picks(payload)
            _persist_internal_detail_report(_internal_detail_stem(market, str(payload.get("generated_at", ""))), detailed)
        client_payload = _merge_payloads(market_payloads)
        source_path = _persist_internal_detail_report(
            _internal_merged_stem(str(client_payload.get("generated_at", ""))),
            OpportunityReportRenderer().render_stock_picks(client_payload),
        )
        client_markdown = ClientReportRenderer().render_stock_picks_detailed(client_payload)
        target_path = FINAL_DIR / f"{_final_stem(str(client_payload.get('generated_at', '')))}.md"

        try:
            from src.commands.release_check import check_stock_pick_client_report

            findings = check_stock_pick_client_report(client_markdown, source_path.read_text(encoding="utf-8"))
            bundle = export_reviewed_markdown_bundle(
                report_type="stock_pick",
                markdown_text=client_markdown,
                markdown_path=target_path,
                release_findings=findings,
                extra_manifest={"market": "all", "detail_source": str(source_path)},
            )
        except (Exception, ReportGuardError) as exc:
            raise SystemExit(str(exc))

        print(client_markdown)
        print(f"\n[client markdown] {bundle['markdown']}")
        print(f"[client pdf] {bundle['pdf']}")
        return

    payload = _run_market(config, args.market, args.top, sector_filter)
    detailed = OpportunityReportRenderer().render_stock_picks(payload)
    detail_path = _persist_internal_detail_report(_internal_detail_stem(args.market, str(payload.get("generated_at", ""))), detailed)
    client_markdown = ClientReportRenderer().render_stock_picks_detailed(payload)
    target_path = FINAL_DIR / f"{_market_final_stem(args.market, str(payload.get('generated_at', '')))}.md"

    findings = []
    if args.market == "cn":
        try:
            from src.commands.release_check import check_stock_pick_client_report

            findings = check_stock_pick_client_report(client_markdown, detail_path.read_text(encoding="utf-8"))
        except Exception as exc:
            raise SystemExit(f"发布前一致性校验失败: {exc}")
    try:
        bundle = export_reviewed_markdown_bundle(
            report_type="stock_pick",
            markdown_text=client_markdown,
            markdown_path=target_path,
            release_findings=findings,
            extra_manifest={"market": args.market, "detail_source": str(detail_path)},
        )
    except ReportGuardError as exc:
        raise SystemExit(str(exc))

    print(client_markdown)
    print(f"\n[client markdown] {bundle['markdown']}")
    print(f"[client pdf] {bundle['pdf']}")


if __name__ == "__main__":
    main()
