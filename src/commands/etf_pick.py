"""Daily ETF recommendation command."""

from __future__ import annotations

import argparse
from typing import Any, Dict, List, Sequence

from src.commands.report_guard import ReportGuardError, ensure_report_task_registered, export_reviewed_markdown_bundle
from src.commands.release_check import check_generic_client_report
from src.output import ClientReportRenderer
from src.output.client_report import _fund_profile_sections
from src.processors.opportunity_engine import discover_opportunities
from src.utils.config import load_config, resolve_project_path
from src.utils.logger import setup_logger


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select today's ETF pick from the Tushare ETF universe.")
    parser.add_argument("theme", nargs="?", default="", help="Optional ETF theme filter, e.g. 红利 / 黄金 / 电网 / 能化")
    parser.add_argument("--top", type=int, default=8, help="Number of ETF analyses to consider")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--client-final", action="store_true", help="Render and persist customer-facing final markdown/pdf")
    return parser


def _score_of(analysis: Dict[str, Any], key: str) -> float:
    return float(dict(analysis.get("dimensions", {}).get(key) or {}).get("score") or 0)


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
        rows.append([label, display, reason])
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


def _winner_reason_lines(analysis: Dict[str, Any]) -> List[str]:
    narrative = dict(analysis.get("narrative") or {})
    reasons: List[str] = []
    reasons.extend(str(item).strip() for item in (narrative.get("positives") or []) if str(item).strip())
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


def _payload_from_analyses(analyses: Sequence[Dict[str, Any]], blind_spots: Sequence[str] | None = None) -> Dict[str, Any]:
    if not analyses:
        raise ValueError("No ETF analyses available")
    ranked = sorted(
        analyses,
        key=lambda item: (
            -int(item.get("rating", {}).get("rank", 0) or 0),
            -_rank_score(item),
        ),
    )
    winner = ranked[0]
    alternatives = ranked[1:3]
    return {
        "generated_at": str(winner.get("generated_at", "")),
        "winner": {
            "name": winner.get("name"),
            "symbol": winner.get("symbol"),
            "trade_state": dict(winner.get("narrative") or {}).get("judgment", {}).get("state", "持有优于追高"),
            "positives": _winner_reason_lines(winner),
            "dimension_rows": _dimension_rows(winner),
            "action": dict(winner.get("action") or {}),
            "positioning_lines": _positioning_lines(winner),
            "evidence": list(dict(winner.get("dimensions", {}).get("catalyst") or {}).get("evidence") or []),
            "fund_sections": _fund_profile_sections(winner),
        },
        "alternatives": [
            {
                "name": item.get("name"),
                "symbol": item.get("symbol"),
                "cautions": _alternative_cautions(item),
            }
            for item in alternatives
        ],
        "notes": [str(item).strip() for item in (blind_spots or []) if str(item).strip()],
    }


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("etf_pick")
    setup_logger("ERROR")
    config = load_config(args.config or None)
    payload = discover_opportunities(config, top_n=max(args.top, 5), theme_filter=args.theme.strip())
    analyses = list(payload.get("top") or [])
    if not analyses:
        raise SystemExit("当前 ETF 推荐池没有可用候选，请稍后重试或放宽主题过滤。")
    client_payload = _payload_from_analyses(analyses, blind_spots=payload.get("blind_spots") or [])
    markdown = ClientReportRenderer().render_etf_pick(client_payload)
    if not args.client_final:
        print(markdown)
        return

    date_str = str(client_payload.get("generated_at", ""))[:10]
    theme = args.theme.strip().replace("/", "_").replace(" ", "_")
    filename = f"etf_pick_{theme}_{date_str}_final.md" if theme else f"etf_pick_{date_str}_final.md"
    findings = check_generic_client_report(markdown, "etf_pick")
    try:
        bundle = export_reviewed_markdown_bundle(
            report_type="etf_pick",
            markdown_text=markdown,
            markdown_path=resolve_project_path(f"reports/etf_picks/final/{filename}"),
            release_findings=findings,
            extra_manifest={
                "theme_filter": args.theme.strip(),
                "winner": dict(client_payload.get("winner") or {}).get("symbol", ""),
            },
        )
    except ReportGuardError as exc:
        raise SystemExit(str(exc))
    print(markdown)
    print(f"\n[client markdown] {bundle['markdown']}")
    print(f"[client pdf] {bundle['pdf']}")


if __name__ == "__main__":
    main()
