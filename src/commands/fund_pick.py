"""Daily off-exchange fund pick command."""

from __future__ import annotations

import argparse
from datetime import datetime
from typing import Any, Dict, List, Sequence

from src.commands.report_guard import ReportGuardError, ensure_report_task_registered, export_reviewed_markdown_bundle
from src.commands.release_check import check_generic_client_report
from src.output import ClientReportRenderer
from src.processors.opportunity_engine import analyze_opportunity, build_market_context
from src.utils.config import load_config, resolve_project_path
from src.utils.logger import setup_logger

DEFAULT_CANDIDATES = ["021740", "022365", "025832"]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select today's off-exchange fund pick.")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument(
        "--candidates",
        default=",".join(DEFAULT_CANDIDATES),
        help="Comma-separated candidate fund symbols",
    )
    parser.add_argument("--client-final", action="store_true", help="Render and persist client-facing final markdown/pdf")
    return parser


def _rank_score(analysis: Dict[str, Any], defensive_mode: bool) -> float:
    dims = analysis.get("dimensions", {})
    technical = float(dims.get("technical", {}).get("score") or 0)
    fundamental = float(dims.get("fundamental", {}).get("score") or 0)
    catalyst = float(dims.get("catalyst", {}).get("score") or 0)
    relative = float(dims.get("relative_strength", {}).get("score") or 0)
    risk = float(dims.get("risk", {}).get("score") or 0)
    macro = float(dims.get("macro", {}).get("score") or 0)
    text_blob = " ".join(
        [
            str(analysis.get("name", "")),
            str(dict(analysis.get("metadata") or {}).get("sector", "")),
            str(dict(analysis.get("fund_profile") or {}).get("overview", {}).get("业绩比较基准", "")),
            " ".join(dict(analysis.get("metadata") or {}).get("fund_style_tags", []) or []),
        ]
    ).lower()
    defensive_bonus = 0.0
    if any(token in text_blob for token in ("黄金", "gold", "避险")) and risk >= 70 and catalyst >= 70:
        defensive_bonus += 30.0
    if defensive_mode:
        return risk * 0.35 + catalyst * 0.25 + macro * 0.15 + technical * 0.15 + relative * 0.10 + defensive_bonus
    return technical * 0.25 + fundamental * 0.20 + catalyst * 0.20 + relative * 0.20 + risk * 0.15


def _fund_dimension_rows(analysis: Dict[str, Any]) -> List[List[str]]:
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


def _score_of(analysis: Dict[str, Any], key: str) -> float:
    return float(dict(analysis.get("dimensions", {}).get(key) or {}).get("score") or 0)


def _winner_reason_lines(analysis: Dict[str, Any], defensive_mode: bool) -> List[str]:
    lines: List[str] = []
    sector = str(dict(analysis.get("metadata") or {}).get("sector", "")).strip()
    action = dict(analysis.get("action") or {})
    trade_state = str(dict(analysis.get("narrative") or {}).get("judgment", {}).get("state", "")).strip()
    catalyst = _score_of(analysis, "catalyst")
    risk = _score_of(analysis, "risk")
    technical = _score_of(analysis, "technical")
    macro = _score_of(analysis, "macro")
    catalyst_reason = str(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("summary", "")).strip()
    risk_reason = str(dict(analysis.get("dimensions", {}).get("risk") or {}).get("summary", "")).strip()
    technical_reason = str(dict(analysis.get("dimensions", {}).get("technical") or {}).get("summary", "")).strip()
    macro_reason = str(dict(analysis.get("dimensions", {}).get("macro") or {}).get("summary", "")).strip()

    if defensive_mode and sector == "黄金":
        lines.append("今天更适合先做防守而不是继续追高贝塔，黄金联接更符合地缘和风险偏好下行时的配置方向。")
    elif defensive_mode:
        lines.append("今天先看防守和回撤控制，这只基金的组合拖累相对更可控。")

    if catalyst >= 70:
        lines.append(f"催化面 `{int(catalyst)}` 分，说明短期驱动并不弱。{catalyst_reason}".strip())
    if risk >= 70:
        lines.append(f"风险特征 `{int(risk)}` 分，意味着它更适合放进今天的组合框架里做防守或平衡。{risk_reason}".strip())
    if macro >= 20:
        lines.append(f"宏观敏感度 `{int(macro)}` 分。{macro_reason or '宏观环境没有明显逆风，这点比单纯看净值涨跌更重要。'}".strip())
    if technical < 50 or trade_state:
        lines.append(
            f"但这不是追涨型机会，当前更适合按 `{trade_state or action.get('direction', '持有优于追高')}` 去做，而不是直接重仓。"
        )

    deduped: List[str] = []
    seen = set()
    for item in lines:
        text = item.strip()
        if not text or text in seen:
            continue
        seen.add(text)
        deduped.append(text)
    return deduped[:4]


def _alternative_cautions(analysis: Dict[str, Any], winner: Dict[str, Any], defensive_mode: bool) -> List[str]:
    cautions = list(dict(analysis.get("narrative") or {}).get("cautions") or [])
    technical = _score_of(analysis, "technical")
    catalyst = _score_of(analysis, "catalyst")
    risk = _score_of(analysis, "risk")
    sector = str(dict(analysis.get("metadata") or {}).get("sector", "")).strip()
    winner_sector = str(dict(winner.get("metadata") or {}).get("sector", "")).strip()

    extra: List[str] = []
    if defensive_mode and winner_sector == "黄金" and sector != "黄金":
        extra.append("今天的主线更偏防守，这只基金的进攻属性更强，放在今天的环境里优先级就会往后排。")
    if technical < 45:
        extra.append("技术结构还不够顺，直接介入更像左侧尝试，不像右侧确认。")
    if catalyst < 60:
        extra.append("短线催化还不足以把下一段行情真正推起来。")
    if risk < 60 and defensive_mode:
        extra.append("回撤和波动承受度不如防守型方案，今天不适合把它放在第一位。")

    merged = []
    seen = set()
    for item in [*cautions, *extra]:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        merged.append(text)
    return merged[:3]


def _payload_from_analyses(analyses: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    if not analyses:
        raise ValueError("No fund analyses available")
    generated_at = str(analyses[0].get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    theme = str(analyses[0].get("day_theme", {}).get("label", ""))
    defensive_mode = any(token in theme for token in ("能源", "风险", "防守", "地缘"))
    if not defensive_mode:
        defensive_mode = any(
            "黄金" in " ".join(
                [
                    str(item.get("name", "")),
                    str(dict(item.get("metadata") or {}).get("sector", "")),
                ]
            )
            for item in analyses
        )
    ranked = sorted(
        analyses,
        key=lambda item: (
            -int(item.get("rating", {}).get("rank", 0) or 0),
            -_rank_score(item, defensive_mode),
        ),
    )
    winner = ranked[0]
    gold_candidates = []
    for item in ranked:
        blob = " ".join(
            [
                str(item.get("name", "")),
                str(dict(item.get("metadata") or {}).get("sector", "")),
                str(dict(item.get("fund_profile") or {}).get("overview", {}).get("业绩比较基准", "")),
            ]
        )
        if "黄金" in blob or "gold" in blob.lower():
            gold_candidates.append(item)
    if gold_candidates:
        gold = gold_candidates[0]
        if (
            float(gold.get("dimensions", {}).get("catalyst", {}).get("score") or 0) >= 70
            and float(gold.get("dimensions", {}).get("risk", {}).get("score") or 0) >= 70
            and (
                float(winner.get("dimensions", {}).get("technical", {}).get("score") or 0) < 50
                or float(winner.get("dimensions", {}).get("catalyst", {}).get("score") or 0) < 60
            )
        ):
            winner = gold
    narrative = dict(winner.get("narrative") or {})
    winner_payload = {
        "name": winner.get("name"),
        "symbol": winner.get("symbol"),
        "trade_state": narrative.get("judgment", {}).get("state", "持有优于追高"),
        "positives": _winner_reason_lines(winner, defensive_mode),
        "dimension_rows": _fund_dimension_rows(winner),
        "action": dict(winner.get("action") or {}),
        "positioning_lines": [
            f"首次仓位按 `{winner.get('action', {}).get('position', '计划仓位的 1/3 - 1/2')}` 执行。",
            f"加仓节奏按 `{winner.get('action', {}).get('scaling_plan', '确认后再考虑第二笔')}` 执行。",
            f"止损参考按 `{winner.get('action', {}).get('stop', '重新跌破关键支撑就处理')}` 管理。",
        ],
    }
    alternatives = []
    for item in ranked[1:3]:
        alternatives.append(
            {
                "name": item.get("name"),
                "symbol": item.get("symbol"),
                "cautions": _alternative_cautions(item, winner, defensive_mode),
            }
        )
    return {
        "generated_at": generated_at,
        "winner": winner_payload,
        "alternatives": alternatives,
    }


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("fund_pick")
    setup_logger("ERROR")
    config = load_config(args.config or None)
    candidates = [item.strip() for item in str(args.candidates).split(",") if item.strip()]
    context = build_market_context(config, relevant_asset_types=["cn_fund", "cn_etf", "futures"])
    analyses = [analyze_opportunity(symbol, "cn_fund", config, context=context) for symbol in candidates]
    payload = _payload_from_analyses(analyses)
    markdown = ClientReportRenderer().render_fund_pick(payload)
    if not args.client_final:
        print(markdown)
        return

    findings = check_generic_client_report(markdown, "fund_pick")
    date_str = str(payload.get("generated_at", ""))[:10]
    try:
        bundle = export_reviewed_markdown_bundle(
            report_type="fund_pick",
            markdown_text=markdown,
            markdown_path=resolve_project_path(f"reports/scans/funds/final/fund_pick_{date_str}_client_final.md"),
            release_findings=findings,
            extra_manifest={"candidates": list(candidates)},
        )
    except ReportGuardError as exc:
        raise SystemExit(str(exc))
    print(markdown)
    print(f"\n[client markdown] {bundle['markdown']}")
    print(f"[client pdf] {bundle['pdf']}")


if __name__ == "__main__":
    main()
