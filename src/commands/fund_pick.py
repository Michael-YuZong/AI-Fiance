"""Daily off-exchange fund pick command."""

from __future__ import annotations

import argparse
from copy import deepcopy
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Mapping, Sequence

from src.commands.pick_history import enrich_pick_payload_with_score_history, grade_pick_delivery, summarize_pick_coverage
from src.commands.pick_visuals import attach_visuals_to_analyses
from src.commands.final_runner import finalize_client_markdown, internal_sidecar_path
from src.commands.report_guard import ensure_report_task_registered, exported_bundle_lines
from src.commands.release_check import check_generic_client_report
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
    build_fund_pick_editor_packet,
    render_financial_editor_prompt,
    summarize_theme_playbook_contract,
    summarize_what_changed_contract,
)
from src.output.event_digest import summarize_event_digest_contract
from src.output.opportunity_report import _dimension_summary_text
from src.output.pick_ranking import portfolio_overlap_bonus, score_band, strategy_confidence_priority
from src.processors.factor_meta import summarize_factor_contracts_from_analyses
from src.processors.portfolio_actions import attach_portfolio_overlap_summaries
from src.processors.opportunity_engine import analyze_opportunity, build_market_context, discover_fund_opportunities
from src.utils.fund_taxonomy import taxonomy_from_analysis, taxonomy_rows
from src.utils.config import load_config, resolve_project_path
from src.utils.logger import setup_logger

DEFAULT_CANDIDATES = ["021740", "022365", "025832"]
STYLE_LABELS = {
    "all": "不限",
    "index": "指数/增强指数",
    "active": "主动权益",
    "commodity": "商品/黄金",
}
SNAPSHOT_PATH = resolve_project_path("data/fund_pick_score_history.json")
MODEL_VERSION = "fund-pick-2026-03-14-candlestick-v5"
MODEL_CHANGELOG = [
    "场外基金推荐现在记录同日基准版和重跑快照，后续重跑会展示分数变化而不是静态覆盖旧稿。",
    "全市场发现模式的候选池、覆盖率和分母定义会同步进入客户稿和内部详细稿，外审门禁同步要求这些章节存在。",
    "催化面在新闻/事件覆盖降级时会按最近一次有效快照做衰减回退，避免把场外基金催化打成假阴性。",
    "技术面新增 `量价/动量背离` 因子，按最近两组确认摆点检查 RSI / MACD / OBV 与价格是否出现顶/底背离。",
    "K 线形态从“单根 K”升级到“最近 1-3 根组合形态”，会识别吞没、星形、三兵三鸦等常见信号，并结合前序 5 日趋势过滤误报。",
]


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
    if not bool(market_context.get("skip_market_drivers")):
        market_context["skip_market_drivers"] = True
        proxy_changed = True
    if proxy_changed:
        effective["market_context"] = market_context
        notes.append("为保证场外基金 `client-final` 可交付，本轮自动跳过跨市场代理、market monitor 与板块驱动慢链。")

    opportunity = dict(effective.get("opportunity") or {})
    current_workers = int(opportunity.get("analysis_workers", 4) or 4)
    if current_workers > 2:
        opportunity["analysis_workers"] = 2
        notes.append("本轮 `client-final` 已自动收窄场外基金分析并发，优先保证正式稿稳定落盘。")
    current_candidates = int(opportunity.get("fund_max_scan_candidates", 12) or 12)
    if current_candidates > 10:
        opportunity["fund_max_scan_candidates"] = 10
        notes.append("本轮 `client-final` 已自动收窄场外基金候选池，优先分析更接近正式交付的高流动性样本。")
    if opportunity:
        effective["opportunity"] = opportunity

    if bool(effective.get("news_topic_search_enabled", True)):
        effective["news_topic_search_enabled"] = False
        notes.append("本轮 `client-final` 已自动关闭场外基金主题新闻扩搜，优先使用结构化事件和已有本地证据。")
    current_news_feeds = str(effective.get("news_feeds_file", "") or "").strip()
    if current_news_feeds != "config/news_feeds.empty.yaml":
        effective["news_feeds_file"] = "config/news_feeds.empty.yaml"
        notes.append("本轮 `client-final` 已自动切到轻量新闻源配置，避免场外基金正式稿被全局新闻拉取慢链拖住。")
    current_profile_timeout = float(effective.get("fund_profile_timeout_seconds", 10) or 10)
    if "fund_profile_timeout_seconds" not in effective or current_profile_timeout > 10:
        effective["fund_profile_timeout_seconds"] = 10
        notes.append("本轮 `client-final` 已自动收紧基金画像慢链超时，超时后按缺失披露，不再无限等待。")
    current_index_bundle_timeout = float(effective.get("index_topic_bundle_timeout_seconds", 10) or 10)
    if "index_topic_bundle_timeout_seconds" not in effective or current_index_bundle_timeout > 10:
        effective["index_topic_bundle_timeout_seconds"] = 10
        notes.append("本轮 `client-final` 已自动收紧指数专题慢链超时，超时后按缺失披露，不再无限等待。")

    return effective, notes


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Select today's off-exchange fund pick.")
    parser.add_argument("--config", default="", help="Optional path to config YAML")
    parser.add_argument("--theme", default="", help="Optional theme filter, e.g. 黄金 / 红利 / 科技 / 电网")
    parser.add_argument(
        "--style",
        choices=tuple(STYLE_LABELS),
        default="all",
        help="Optional fund style filter: index / active / commodity / all",
    )
    parser.add_argument("--manager", default="", help="Optional fund company keyword filter, e.g. 易方达 / 永赢")
    parser.add_argument("--top", type=int, default=8, help="Number of discovered funds to keep after full analysis")
    parser.add_argument("--pool-size", type=int, default=12, help="Number of pre-screened funds to run full analysis on")
    parser.add_argument(
        "--candidates",
        default="",
        help="Optional comma-separated candidate override; when omitted, use full-universe discovery",
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


def _score_of(analysis: Dict[str, Any], key: str) -> float:
    return float(dict(analysis.get("dimensions", {}).get(key) or {}).get("score") or 0)


def _defensive_mode(analyses: Sequence[Dict[str, Any]]) -> bool:
    if not analyses:
        return False
    theme = str(analyses[0].get("day_theme", {}).get("label", ""))
    defensive_mode = any(token in theme for token in ("能源", "风险", "防守", "地缘"))
    if defensive_mode:
        return True
    return any(
        "黄金" in " ".join(
            [
                str(item.get("name", "")),
                str(dict(item.get("metadata") or {}).get("sector", "")),
            ]
        )
        for item in analyses
    )


def _rank_key(analysis: Mapping[str, Any], defensive_mode: bool) -> tuple[float, float, float, float, float, float]:
    item = dict(analysis)
    rank_score = _rank_score(item, defensive_mode)
    return (
        float(int(item.get("rating", {}).get("rank", 0) or 0)),
        float(score_band(rank_score)),
        float(portfolio_overlap_bonus(item)),
        rank_score,
        3 - strategy_confidence_priority(item),
        _score_of(item, "risk"),
        _score_of(item, "catalyst"),
    )


def _winner_reason_lines(analysis: Dict[str, Any], defensive_mode: bool) -> List[str]:
    lines: List[str] = []
    sector = str(dict(analysis.get("metadata") or {}).get("sector", "")).strip()
    action = dict(analysis.get("action") or {})
    horizon = dict(action.get("horizon") or {})
    trade_state = str(dict(analysis.get("narrative") or {}).get("judgment", {}).get("state", "")).strip()
    catalyst = _score_of(analysis, "catalyst")
    risk = _score_of(analysis, "risk")
    technical = _score_of(analysis, "technical")
    macro = _score_of(analysis, "macro")
    catalyst_reason = str(dict(analysis.get("dimensions", {}).get("catalyst") or {}).get("summary", "")).strip()
    risk_reason = str(dict(analysis.get("dimensions", {}).get("risk") or {}).get("summary", "")).strip()
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
        lines.append(f"但这不是追涨型机会，当前更适合按 `{trade_state or action.get('direction', '持有优于追高')}`` 去做，而不是直接重仓。".replace("``", "`"))
    if horizon.get("fit_reason"):
        lines.append(f"周期上更适合按 `{horizon.get('label', '当前周期')}` 理解：{horizon.get('fit_reason')}")

    if len(lines) < 3:
        relative = _score_of(analysis, "relative_strength")
        relative_reason = str(dict(analysis.get("dimensions", {}).get("relative_strength") or {}).get("summary", "")).strip()
        if relative_reason:
            lines.append(f"相对强弱 `{int(relative)}` 分。{relative_reason}")
    if len(lines) < 3:
        fundamental = _score_of(analysis, "fundamental")
        fundamental_reason = str(dict(analysis.get("dimensions", {}).get("fundamental") or {}).get("summary", "")).strip()
        if fundamental_reason:
            lines.append(f"基本面 `{int(fundamental)}` 分。{fundamental_reason}")
    if len(lines) < 3:
        lines.append("这只基金并不是强进攻型机会，但在今天的全市场初筛里，综合优先级仍然排在前面。")

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
    horizon = dict(dict(analysis.get("action") or {}).get("horizon") or {})
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
    if horizon.get("misfit_reason"):
        extra.append(f"周期上更像 `{horizon.get('label', '观察期')}`：{horizon.get('misfit_reason')}")

    merged = []
    seen = set()
    for item in [*cautions, *extra]:
        text = str(item).strip()
        if not text or text in seen:
            continue
        seen.add(text)
        merged.append(text)
    return merged[:3]


def _detail_output_path(generated_at: str) -> Path:
    date_str = generated_at[:10] or datetime.now().strftime("%Y-%m-%d")
    return resolve_project_path(f"reports/scans/funds/internal/fund_pick_{date_str}_internal_detail.md")


def _candidate_summary_rows(analyses: Sequence[Dict[str, Any]], defensive_mode: bool) -> List[List[str]]:
    rows: List[List[str]] = []
    for item in analyses:
        rating = dict(item.get("rating") or {})
        narrative = dict(item.get("narrative") or {})
        horizon = dict(dict(item.get("action") or {}).get("horizon") or {})
        rows.append(
            [
                f"{item.get('name', '—')} ({item.get('symbol', '—')})",
                f"{rating.get('stars', '—')} {rating.get('label', '未评级')}",
                f"{_rank_score(item, defensive_mode):.1f}",
                str(dict(narrative.get('judgment') or {}).get("state", "观察为主")),
                str(horizon.get("label", dict(item.get("action") or {}).get("timeframe", "观察期"))).replace("(", "（").replace(")", "）"),
            ]
        )
    return rows


def _discovery_mode_label(mode: str) -> str:
    return {
        "manual_candidates": "手动候选",
        "full_universe": "全市场初筛",
        "default_candidates_fallback": "默认候选回退",
    }.get(str(mode), str(mode) or "未标注")


def _selection_context(
    *,
    discovery_mode: str,
    scan_pool: int,
    passed_pool: int,
    theme_filter: str = "",
    style_filter: str = "all",
    manager_filter: str = "",
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
        "style_filter_label": STYLE_LABELS.get(style_filter, STYLE_LABELS["all"]),
        "manager_filter_label": manager_filter or "未指定",
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


def _detail_markdown(
    analyses: Sequence[Dict[str, Any]],
    winner_symbol: str,
    *,
    selection_context: Mapping[str, Any] | None = None,
) -> str:
    defensive_mode = _defensive_mode(analyses)
    ranked = sorted(analyses, key=lambda item: _rank_key(item, defensive_mode), reverse=True)
    winner = next((item for item in ranked if str(item.get("symbol", "")) == winner_symbol), ranked[0])
    alternatives = [item for item in ranked if str(item.get("symbol", "")) != str(winner_symbol)]
    generated_at = str(winner.get("generated_at", ""))[:10]
    selection = dict(selection_context or {})
    lines = [
        f"# 今日场外基金推荐内部详细稿 | {generated_at}",
        "",
        f"- 交付等级: `{selection.get('delivery_tier_label', '未标注')}`",
        f"- 发现方式: `{selection.get('discovery_mode_label', '未标注')}`",
        f"- 初筛基金池: `{selection.get('scan_pool', len(analyses))}`",
        f"- 进入完整分析: `{selection.get('passed_pool', len(analyses))}`",
        f"- 主题过滤: `{selection.get('theme_filter_label', '未指定')}`",
        f"- 风格过滤: `{selection.get('style_filter_label', STYLE_LABELS['all'])}`",
        f"- 管理人过滤: `{selection.get('manager_filter_label', '未指定')}`",
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
    lines.extend(_table(["标的", "评级", "排序分", "交易状态", "周期"], _candidate_summary_rows(ranked[:5], defensive_mode)))
    lines.extend(
        [
            "",
            "## 中选说明",
            "",
            f"- 中选标的：`{winner.get('name', '—')} ({winner.get('symbol', '—')})`。",
            f"- 当前模式：`{'防守优先' if defensive_mode else '平衡/进攻优先'}`，客户稿评分表会与下方详细稿八维评分保持硬一致。",
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
            lines.append(f"- `{item.get('name', '—')} ({item.get('symbol', '—')})` 仍在观察池，但今天综合优先级低于中选标的。")
    if selection.get("blind_spots"):
        lines.extend(["", "## 数据盲区与降级说明", ""])
        for item in selection.get("blind_spots", [])[:5]:
            text = str(item).strip()
            if text:
                lines.append(f"- {text}")
    lines.extend(["", "## 中选标的详细分析", ""])
    lines.append(OpportunityReportRenderer().render_scan(dict(winner)).rstrip())
    return "\n".join(lines).rstrip() + "\n"


def _payload_from_analyses(
    analyses: Sequence[Dict[str, Any]],
    selection_context: Dict[str, Any] | None = None,
    *,
    regime: Mapping[str, Any] | None = None,
    day_theme: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    if not analyses:
        raise ValueError("No fund analyses available")
    generated_at = str(analyses[0].get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S")))
    defensive_mode = _defensive_mode(analyses)
    ranked = sorted(analyses, key=lambda item: _rank_key(item, defensive_mode), reverse=True)
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
        "asset_type": winner.get("asset_type"),
        "generated_at": winner.get("generated_at"),
        "strategy_background_confidence": dict(winner.get("strategy_background_confidence") or {}),
        "portfolio_overlap_summary": dict(winner.get("portfolio_overlap_summary") or {}),
        "visuals": dict(winner.get("visuals") or {}),
        "reference_price": float(dict(winner.get("metrics") or {}).get("last_close") or 0.0),
        "trade_state": narrative.get("judgment", {}).get("state", "持有优于追高"),
        "positives": _winner_reason_lines(winner, defensive_mode),
        "dimension_rows": _fund_dimension_rows(winner),
        "dimensions": dict(winner.get("dimensions") or {}),
        "action": dict(winner.get("action") or {}),
        "provenance": dict(winner.get("provenance") or {}),
        "intraday": dict(winner.get("intraday") or {}),
        "metadata": dict(winner.get("metadata") or {}),
        "history": winner.get("history"),
        "benchmark_name": winner.get("benchmark_name"),
        "benchmark_symbol": winner.get("benchmark_symbol"),
        "narrative": {"playbook": dict(narrative.get("playbook") or {})},
        "positioning_lines": [
            f"首次仓位按 `{winner.get('action', {}).get('position', '计划仓位的 1/3 - 1/2')}` 执行。",
            f"加仓节奏按 `{winner.get('action', {}).get('scaling_plan', '确认后再考虑第二笔')}` 执行。",
            f"止损参考按 `{winner.get('action', {}).get('stop', '重新跌破关键支撑就处理')}` 管理。",
        ],
        "taxonomy_rows": taxonomy_rows(taxonomy_from_analysis(winner)),
        "taxonomy_summary": str(taxonomy_from_analysis(winner).get("summary", "")),
        "score_changes": list(winner.get("score_changes") or []),
        "comparison_basis_label": str(winner.get("comparison_basis_label", "")),
        "comparison_snapshot_at": str(winner.get("comparison_snapshot_at", "")),
        "proxy_signals": dict(winner.get("proxy_signals") or {}),
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
        "selection_context": dict(selection_context or {}),
        "regime": dict(regime or {}),
        "day_theme": dict(day_theme or {}),
    }


def _scope_key(*, discovery_mode: str, theme_filter: str, style_filter: str, manager_filter: str) -> str:
    return "|".join(
        [
            f"mode={discovery_mode or 'all'}",
            f"theme={theme_filter or '*'}",
            f"style={style_filter or 'all'}",
            f"manager={manager_filter or '*'}",
        ]
    )


def main() -> None:
    args = build_parser().parse_args()
    ensure_report_task_registered("fund_pick")
    setup_logger("ERROR")
    base_config = load_config(args.config or None)
    config, runtime_notes = _client_final_runtime_overrides(
        base_config,
        client_final=bool(args.client_final),
        explicit_config_path=str(args.config or ""),
    )
    blind_spots: List[str] = []
    scan_pool = 0
    passed_pool = 0
    theme_filter = args.theme.strip()
    style_filter = str(args.style).strip().lower()
    manager_filter = args.manager.strip()
    candidates = [item.strip() for item in str(args.candidates).split(",") if item.strip()]
    discovery_mode = "manual_candidates" if candidates else "full_universe"

    if candidates:
        if theme_filter or style_filter != "all" or manager_filter:
            blind_spots.append("当前使用手动候选模式，主题/风格/管理人参数不会重筛基金池，只用于记录本次偏好。")
        context = build_market_context(config, relevant_asset_types=["cn_fund", "cn_etf", "futures"])
        analyses = [analyze_opportunity(symbol, "cn_fund", config, context=context) for symbol in candidates]
        scan_pool = len(candidates)
        passed_pool = len(analyses)
        payload: Dict[str, Any] = {
            "generated_at": str(analyses[0].get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) if analyses else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            "scan_pool": scan_pool,
            "passed_pool": passed_pool,
            "top": analyses,
            "coverage_analyses": analyses,
            "blind_spots": blind_spots,
            "data_coverage": summarize_pick_coverage(analyses),
        }
    else:
        pool_size = max(int(args.pool_size), 5)
        runtime_pool_cap = int(dict(config.get("opportunity") or {}).get("fund_max_scan_candidates", pool_size) or pool_size)
        payload = discover_fund_opportunities(
            config,
            top_n=max(int(args.top), 5),
            theme_filter=theme_filter,
            max_candidates=min(pool_size, runtime_pool_cap),
            style_filter=style_filter,
            manager_filter=manager_filter,
        )
        analyses = list(payload.get("top") or [])
        blind_spots = list(payload.get("blind_spots") or [])
        scan_pool = int(payload.get("scan_pool") or 0)
        passed_pool = int(payload.get("passed_pool") or 0)
        candidates = [str(item.get("symbol", "")) for item in analyses if str(item.get("symbol", "")).strip()]
        if not analyses:
            blind_spots.append("全市场场外基金池没有留下可用候选，已回退到默认候选池。")
            discovery_mode = "default_candidates_fallback"
            candidates = list(DEFAULT_CANDIDATES)
            context = build_market_context(config, relevant_asset_types=["cn_fund", "cn_etf", "futures"])
            analyses = [analyze_opportunity(symbol, "cn_fund", config, context=context) for symbol in candidates]
            scan_pool = len(candidates)
            passed_pool = len(analyses)
            payload = {
                "generated_at": str(analyses[0].get("generated_at", datetime.now().strftime("%Y-%m-%d %H:%M:%S"))) if analyses else datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "scan_pool": scan_pool,
                "passed_pool": passed_pool,
                "top": analyses,
                "coverage_analyses": analyses,
                "blind_spots": blind_spots,
                "data_coverage": summarize_pick_coverage(analyses),
            }

    if runtime_notes:
        blind_spots = [*runtime_notes, *blind_spots]

    defensive_mode = _defensive_mode(analyses)
    payload = enrich_pick_payload_with_score_history(
        payload,
        scope=_scope_key(
            discovery_mode=discovery_mode,
            theme_filter=theme_filter,
            style_filter=style_filter,
            manager_filter=manager_filter,
        ),
        snapshot_path=SNAPSHOT_PATH,
        model_version=MODEL_VERSION,
        model_changelog=MODEL_CHANGELOG,
        rank_key=lambda item: _rank_key(item, defensive_mode),
    )
    payload["top"] = _attach_strategy_background_confidence(payload.get("top") or [])
    payload["coverage_analyses"] = _attach_strategy_background_confidence(payload.get("coverage_analyses") or [])
    payload["watch_positive"] = _attach_strategy_background_confidence(payload.get("watch_positive") or [])
    payload["top"] = attach_portfolio_overlap_summaries(payload.get("top") or [], config)
    payload["coverage_analyses"] = attach_portfolio_overlap_summaries(payload.get("coverage_analyses") or [], config)
    payload["watch_positive"] = attach_portfolio_overlap_summaries(payload.get("watch_positive") or [], config)
    analyses = list(payload.get("top") or [])
    attach_visuals_to_analyses(analyses[:3])
    delivery_tier = grade_pick_delivery(
        report_type="fund_pick",
        discovery_mode=discovery_mode,
        coverage=payload.get("pick_coverage") or payload.get("data_coverage") or {},
        scan_pool=scan_pool,
        passed_pool=passed_pool,
        winner=analyses[0] if analyses else None,
    )
    selection_context = _selection_context(
        discovery_mode=discovery_mode,
        scan_pool=scan_pool,
        passed_pool=passed_pool,
        theme_filter=theme_filter,
        style_filter=style_filter,
        manager_filter=manager_filter,
        blind_spots=blind_spots,
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
    report_payload = _payload_from_analyses(
        analyses,
        selection_context=selection_context,
        regime=payload.get("regime") or {},
        day_theme=payload.get("day_theme") or {},
    )
    delivery_tier = grade_pick_delivery(
        report_type="fund_pick",
        discovery_mode=discovery_mode,
        coverage=payload.get("pick_coverage") or payload.get("data_coverage") or {},
        scan_pool=scan_pool,
        passed_pool=passed_pool,
        winner=dict(report_payload.get("winner") or {}),
    )
    selection_context = _selection_context(
        discovery_mode=discovery_mode,
        scan_pool=scan_pool,
        passed_pool=passed_pool,
        theme_filter=theme_filter,
        style_filter=style_filter,
        manager_filter=manager_filter,
        blind_spots=blind_spots,
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
    date_str = str(report_payload.get("generated_at", ""))[:10]
    detail_path = _detail_output_path(str(report_payload.get("generated_at", "")))
    if args.client_final:
        catalyst_review_path = internal_sidecar_path(detail_path, "catalyst_web_review.md")
        review_lookup = load_catalyst_web_review(catalyst_review_path)
        if review_lookup:
            analyses = [attach_catalyst_web_review_to_analysis(item, review_lookup) for item in analyses]
            report_payload = _payload_from_analyses(
                analyses,
                selection_context=selection_context,
                regime=payload.get("regime") or {},
                day_theme=payload.get("day_theme") or {},
            )
    report_payload["selection_context"] = selection_context
    markdown = ClientReportRenderer().render_fund_pick(report_payload)
    if not args.client_final:
        print(markdown)
        return

    markdown_path = resolve_project_path(f"reports/scans/funds/final/fund_pick_{date_str}_client_final.md")
    detail_markdown = _detail_markdown(
        analyses,
        str(dict(report_payload.get("winner") or {}).get("symbol", "")),
        selection_context=selection_context,
    )
    factor_contract = summarize_factor_contracts_from_analyses(list(payload.get("coverage_analyses") or analyses), sample_limit=16)
    catalyst_review_path = internal_sidecar_path(detail_path, "catalyst_web_review.md")
    editor_packet = build_fund_pick_editor_packet(report_payload)
    editor_prompt = render_financial_editor_prompt(editor_packet)
    catalyst_packet = build_catalyst_web_review_packet(
        report_type="fund_pick",
        subject=f"fund_pick {date_str}",
        generated_at=str(report_payload.get("generated_at", "")),
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
        report_type="fund_pick",
        client_markdown=markdown,
        markdown_path=markdown_path,
        detail_markdown=detail_markdown,
        detail_path=detail_path,
        extra_manifest={
            "candidates": list(candidates),
            "winner": dict(report_payload.get("winner") or {}).get("symbol", ""),
            "theme_filter": theme_filter,
            "style_filter": style_filter,
            "manager_filter": manager_filter,
            "scan_pool": scan_pool,
            "passed_pool": passed_pool,
            "discovery_mode": discovery_mode,
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
            "fund_pick",
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


if __name__ == "__main__":
    main()
