"""Stock pick command — scan stock universe and surface top individual stock picks."""

from __future__ import annotations

import argparse
from pathlib import Path
from typing import Any, Dict, Mapping, Optional

from src.commands.pick_history import enrich_pick_payload_with_score_history, summarize_pick_coverage
from src.commands.pick_visuals import attach_visuals_to_analyses
from src.commands.report_guard import ReportGuardError, ensure_report_task_registered, export_reviewed_markdown_bundle, exported_bundle_lines
from src.output import ClientReportRenderer, OpportunityReportRenderer
from src.processors.factor_meta import summarize_factor_contracts_from_analyses
from src.processors.opportunity_engine import build_market_context, discover_stock_opportunities, summarize_proxy_contracts_from_analyses
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


def _factor_contract_summary(analyses: list[Mapping[str, Any]]) -> Dict[str, Any]:
    return summarize_factor_contracts_from_analyses(list(analyses or []), sample_limit=16)


def _attach_featured_visuals(payload: Dict[str, Any]) -> Dict[str, Any]:
    top = list(payload.get("top") or [])
    if not top:
        return payload
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

    featured: list[Dict[str, Any]] = []
    for market_name in ("A股", "港股", "美股"):
        items = grouped.get(market_name) or []
        if not items:
            continue
        ranked = ClientReportRenderer._rank_market_items(items, watch_symbols)
        featured.extend([dict(item) if not isinstance(item, dict) else item for item in ranked[:3]])
    attach_visuals_to_analyses(featured)
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
) -> Dict[str, Any]:
    payload = discover_stock_opportunities(config, top_n=top_n, market=market, sector_filter=sector_filter, context=context)
    payload = enrich_payload_with_score_history(payload, market=market, sector_filter=sector_filter)
    return _attach_featured_visuals(payload)


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("stock_pick")
    setup_logger("ERROR")
    config = load_config(args.config or None)
    sector_filter = args.sector.strip()
    try:
        if not args.client_final:
            payload = discover_stock_opportunities(config, top_n=args.top, market=args.market, sector_filter=sector_filter)
            payload = enrich_payload_with_score_history(payload, market=args.market, sector_filter=sector_filter)
            print(OpportunityReportRenderer().render_stock_picks(payload))
            return

        if args.market == "all":
            shared_context = build_market_context(
                config,
                relevant_asset_types=["cn_stock", "cn_etf", "hk", "us", "futures"],
            )
            market_payloads = {
                market: _run_market(config, market, args.top, sector_filter, context=shared_context)
                for market in ("cn", "hk", "us")
            }
            for market, payload in market_payloads.items():
                detailed = OpportunityReportRenderer().render_stock_picks(payload)
                _persist_internal_detail_report(_internal_detail_stem(market, str(payload.get("generated_at", ""))), detailed)
            client_payload = _merge_payloads(market_payloads)
            factor_contract = _factor_contract_summary(
                [
                    analysis
                    for market_payload in market_payloads.values()
                    for analysis in list(market_payload.get("coverage_analyses") or market_payload.get("top") or [])
                ]
            )
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
                    extra_manifest={
                        "market": "all",
                        "detail_source": str(source_path),
                        "factor_contract": factor_contract,
                        "proxy_contract": dict(client_payload.get("proxy_contract") or {}),
                    },
                )
            except (Exception, ReportGuardError) as exc:
                raise SystemExit(str(exc))

            print(client_markdown)
            for index, line in enumerate(exported_bundle_lines(bundle)):
                print(f"\n{line}" if index == 0 else line)
            return

        payload = _run_market(config, args.market, args.top, sector_filter)
        detailed = OpportunityReportRenderer().render_stock_picks(payload)
        detail_path = _persist_internal_detail_report(_internal_detail_stem(args.market, str(payload.get("generated_at", ""))), detailed)
        client_markdown = ClientReportRenderer().render_stock_picks_detailed(payload)
        target_path = FINAL_DIR / f"{_market_final_stem(args.market, str(payload.get('generated_at', '')))}.md"
        factor_contract = _factor_contract_summary(list(payload.get("coverage_analyses") or payload.get("top") or []))

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
                extra_manifest={
                    "market": args.market,
                    "detail_source": str(detail_path),
                    "factor_contract": factor_contract,
                    "proxy_contract": dict(payload.get("proxy_contract") or {}),
                },
            )
        except ReportGuardError as exc:
            raise SystemExit(str(exc))

        print(client_markdown)
        for index, line in enumerate(exported_bundle_lines(bundle)):
            print(f"\n{line}" if index == 0 else line)
    finally:
        close_yfinance_runtime_caches()


if __name__ == "__main__":
    main()
