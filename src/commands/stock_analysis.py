"""Single-stock detailed analysis command."""

from __future__ import annotations

import argparse
from copy import deepcopy
from pathlib import Path
from typing import Dict, List, Mapping, Tuple

from src.commands.final_runner import finalize_client_markdown, internal_sidecar_path
from src.commands.intel import collect_intel_news_report, collect_market_aware_intel_news_report
from src.commands.report_guard import ensure_report_task_registered, exported_bundle_lines
from src.commands.release_check import check_generic_client_report
from src.output import AnalysisChartRenderer, ClientReportRenderer, OpportunityReportRenderer
from src.output.ai_editor import ai_editor_run_payload, maybe_apply_ai_editor
from src.output.catalyst_web_review import (
    attach_catalyst_web_review_to_analysis,
    build_catalyst_web_review_packet,
    load_catalyst_web_review,
    render_catalyst_web_review_prompt,
    render_catalyst_web_review_scaffold,
)
from src.output.client_report import _recommendation_bucket
from src.output.editor_payload import (
    build_stock_analysis_editor_packet,
    render_financial_editor_prompt,
    summarize_theme_playbook_contract,
    summarize_what_changed_contract,
)
from src.output.event_digest import summarize_event_digest_contract
from src.processors.opportunity_engine import _attach_signal_confidence, analyze_opportunity, build_market_context
from src.processors.portfolio_actions import build_candidate_portfolio_overlap_summary
from src.utils.market import resolve_asset_context
from src.utils.config import detect_asset_type, load_config, resolve_project_path
from src.utils.logger import setup_logger


def _shared_intel_news_report(
    config: Mapping[str, object],
    *,
    query: str,
    explicit_symbol: str = "",
    baseline_report: Mapping[str, object] | None = None,
    limit: int = 6,
    recent_days: int = 7,
    note_prefix: str = "",
) -> Dict[str, object]:
    try:
        report = collect_market_aware_intel_news_report(
            query,
            config=config,
            explicit_symbol=explicit_symbol,
            baseline_report=baseline_report,
            limit=limit,
            recent_days=recent_days,
            structured_only=not bool(dict(config or {}).get("news_topic_search_enabled", True)),
            note_prefix=note_prefix,
            collect_fn=collect_intel_news_report,
        )
    except Exception:
        return {}
    if not list(dict(report).get("items") or []):
        return {}
    return dict(report)


def _attach_shared_intel_news_report(
    context: Mapping[str, object],
    config: Mapping[str, object],
    *,
    query: str,
    explicit_symbol: str = "",
    note_prefix: str = "",
) -> Dict[str, object]:
    merged = dict(context or {})
    existing_report = dict(merged.get("news_report") or {})
    shared_report = _shared_intel_news_report(
        config,
        query=query,
        explicit_symbol=explicit_symbol,
        baseline_report=existing_report,
        note_prefix=note_prefix,
    )
    if not shared_report:
        return merged

    existing_items = list(existing_report.get("items") or [])
    shared_items = list(shared_report.get("items") or [])
    if not existing_items or len(shared_items) >= len(existing_items):
        merged["news_report"] = shared_report
        merged["intel_news_report"] = shared_report
    return merged


def _client_final_runtime_overrides(
    config: Mapping[str, object],
    *,
    client_final: bool,
    explicit_config_path: str = "",
) -> tuple[Dict[str, object], List[str]]:
    if not client_final or explicit_config_path.strip():
        return deepcopy(dict(config or {})), []

    effective = deepcopy(dict(config or {}))
    notes: List[str] = []
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
        notes.append("为保证个股详细稿 `client-final` 可交付，本轮自动跳过跨市场代理与 market monitor 慢链。")
    if bool(effective.get("news_topic_search_enabled", True)):
        notes.append("本轮 `client-final` 保留主题情报扩搜能力，只对全局新闻源走轻量配置，避免把市场级新情报静默降成空白。")

    current_news_feeds = str(effective.get("news_feeds_file", "") or "").strip()
    if current_news_feeds != "config/news_feeds.empty.yaml":
        effective["news_feeds_file"] = "config/news_feeds.empty.yaml"
        notes.append("本轮 `client-final` 已自动切到轻量新闻源配置，避免个股详细稿被全局新闻拉取慢链拖住。")

    return effective, notes


def run_stock_analysis(
    symbol: str,
    config_path: str = "",
    today_mode: bool = False,
    *,
    client_final: bool = False,
) -> Tuple[str, Dict[str, object]]:
    base_config = load_config(config_path or None)
    config, runtime_notes = _client_final_runtime_overrides(
        base_config,
        client_final=client_final,
        explicit_config_path=config_path,
    )
    setup_logger("ERROR")
    resolved_context = resolve_asset_context(symbol, config)
    symbol = resolved_context.symbol
    asset_type = resolved_context.asset_type or detect_asset_type(symbol, config)
    if asset_type not in {"cn_stock", "hk", "us"}:
        raise SystemExit(f"`{symbol}` 当前识别为 `{asset_type}`，不属于个股分析对象。")
    effective_today_mode = bool(today_mode or (client_final and asset_type == "cn_stock"))
    context = build_market_context(config, relevant_asset_types=[asset_type, "cn_stock", "hk", "us"])
    intel_query_parts = [str(resolved_context.name or symbol).strip(), symbol]
    sector = str((resolved_context.metadata or {}).get("sector", "")).strip()
    if sector:
        intel_query_parts.append(sector)
    context = _attach_shared_intel_news_report(
        context,
        config,
        query=" ".join(dict.fromkeys(part for part in intel_query_parts if part)),
        explicit_symbol=symbol,
        note_prefix="stock_analysis intel: ",
    )
    analysis = analyze_opportunity(symbol, asset_type, config, context=context, today_mode=effective_today_mode)
    _attach_signal_confidence([analysis], config, limit=1)
    analysis["portfolio_overlap_summary"] = build_candidate_portfolio_overlap_summary(analysis, config)
    if runtime_notes:
        notes = [str(item).strip() for item in list(analysis.get("notes") or []) if str(item).strip()]
        for item in runtime_notes:
            if item not in notes:
                notes.append(item)
        analysis["notes"] = notes
    visuals = AnalysisChartRenderer(render_theme_variants=client_final).render(analysis)
    analysis["visuals"] = visuals
    report = OpportunityReportRenderer().render_scan(analysis, visuals=visuals)
    return report, analysis


def _client_output_path(symbol: str, generated_at: str) -> Path:
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    date_str = generated_at[:10]
    return resolve_project_path(f"reports/stock_analysis/final/stock_analysis_{safe_symbol}_{date_str}_final.md")


def _detail_output_path(symbol: str, generated_at: str) -> Path:
    safe_symbol = str(symbol).replace("/", "_").replace(" ", "_")
    date_str = generated_at[:10]
    return resolve_project_path(f"reports/stock_analysis/internal/stock_analysis_{safe_symbol}_{date_str}_internal_detail.md")


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Generate a detailed analysis report for a single stock.")
    parser.add_argument("symbol", help="Stock symbol, e.g. 300750 / 0700.HK / META")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--today", action="store_true", help="Add intraday/today snapshot on top of the default daily scan.")
    parser.add_argument("--client-final", action="store_true", help="Render and persist customer-facing final markdown/pdf")
    return parser


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("stock_analysis")
    report, analysis = run_stock_analysis(args.symbol, args.config, today_mode=args.today, client_final=args.client_final)
    if not args.client_final:
        print(report)
        return

    resolved_symbol = str(analysis.get("symbol", "") or args.symbol)
    detail_path = _detail_output_path(resolved_symbol, str(analysis.get("generated_at", "")))
    catalyst_review_path = internal_sidecar_path(detail_path, "catalyst_web_review.md")
    review_lookup = load_catalyst_web_review(catalyst_review_path)
    analysis_for_client = attach_catalyst_web_review_to_analysis(analysis, review_lookup)
    rule_based_markdown = ClientReportRenderer().render_stock_analysis_detailed(analysis_for_client)
    editor_packet = build_stock_analysis_editor_packet(
        {
            **analysis_for_client,
            "editor_bucket": _recommendation_bucket(analysis_for_client),
        }
    )
    editor_prompt = render_financial_editor_prompt(editor_packet)
    editor_result = maybe_apply_ai_editor(
        rule_based_markdown,
        prompt_text=editor_prompt,
        packet=editor_packet,
    )
    client_markdown = editor_result.markdown
    editor_response_markdown = (
        editor_result.response_markdown
        or "\n".join(
            [
                "# AI Editor Response",
                "",
                f"- applied: {'yes' if editor_result.applied else 'no'}",
                f"- provider: {editor_result.provider or 'none'}",
                f"- model: {editor_result.model or '—'}",
                f"- reason: {editor_result.reason}",
                "",
            ]
        )
    )
    catalyst_packet = build_catalyst_web_review_packet(
        report_type="stock_analysis",
            subject=f"{analysis.get('name', '')} ({resolved_symbol})",
        generated_at=str(analysis.get("generated_at", "")),
        analyses=[analysis_for_client],
    )
    text_sidecars = {
        "editor_prompt": (
            internal_sidecar_path(detail_path, "editor_prompt.md"),
            editor_prompt,
        ),
        "editor_response": (
            internal_sidecar_path(detail_path, "editor_response.md"),
            editor_response_markdown,
        ),
    }
    json_sidecars = {
        "editor_payload": (
            internal_sidecar_path(detail_path, "editor_payload.json"),
            editor_packet,
        ),
        "editor_run": (
            internal_sidecar_path(detail_path, "editor_run.json"),
            ai_editor_run_payload(editor_result, editor_packet),
        ),
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
        report_type="stock_analysis",
        client_markdown=client_markdown,
        markdown_path=_client_output_path(resolved_symbol, str(analysis.get("generated_at", ""))),
        detail_markdown=report,
        detail_path=detail_path,
        extra_manifest={
            "symbol": resolved_symbol,
            "asset_type": str(analysis.get("asset_type", "")),
            "editor_run": ai_editor_run_payload(editor_result, editor_packet),
            "theme_playbook_contract": summarize_theme_playbook_contract(editor_packet.get("theme_playbook") or {}),
            "event_digest_contract": summarize_event_digest_contract(editor_packet.get("event_digest") or {}),
            "what_changed_contract": summarize_what_changed_contract(editor_packet.get("what_changed") or {}),
        },
        release_checker=lambda markdown, source_text: check_generic_client_report(
            markdown,
            "stock_analysis",
            source_text=source_text,
            editor_theme_playbook=editor_packet.get("theme_playbook") or {},
            editor_prompt_text=editor_prompt,
            event_digest_contract=editor_packet.get("event_digest") or {},
            what_changed_contract=editor_packet.get("what_changed") or {},
        ),
        text_sidecars=text_sidecars,
        json_sidecars=json_sidecars,
    )
    print(client_markdown)
    for index, line in enumerate(exported_bundle_lines(bundle)):
        print(f"\n{line}" if index == 0 else line)


if __name__ == "__main__":
    main()
