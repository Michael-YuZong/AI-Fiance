"""Shared snapshot/history helpers for pick pipelines."""

from __future__ import annotations

import json
import re
from datetime import datetime
from pathlib import Path
from typing import Any, Callable, Dict, Mapping, Optional, Sequence

from src.processors.opportunity_engine import _rating_from_dimensions
from src.utils.data import load_json, save_json

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


def summarize_pick_coverage(analyses: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    rows = list(analyses or [])
    if not rows:
        return {
            "news_mode": "unknown",
            "degraded": False,
            "structured_rate": 0.0,
            "direct_news_rate": 0.0,
            "total": 0,
            "note": "当前没有可统计的候选样本。",
            "lines": [],
        }

    modes = [
        str(dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {}).get("news_mode", "unknown"))
        for item in rows
    ]
    news_mode = "live" if modes and all(mode == "live" for mode in modes) else ("proxy" if "proxy" in modes else (modes[0] if modes else "unknown"))
    structured_count = 0
    direct_count = 0
    theme_count = 0
    degraded_count = 0
    for item in rows:
        asset_type = str(item.get("asset_type", "")).strip()
        coverage = dict(dict(item.get("dimensions", {}).get("catalyst") or {}).get("coverage") or {})
        if coverage.get("structured_event") or coverage.get("forward_event"):
            structured_count += 1
        elif asset_type in {"cn_etf", "cn_fund", "cn_index"} and (
            coverage.get("directional_catalyst_hit")
            or int(coverage.get("theme_news_count") or 0) > 0
            or int(coverage.get("recent_theme_news_pool_count") or 0) > 0
        ):
            structured_count += 1
            theme_count += 1
        if coverage.get("high_confidence_company_news"):
            direct_count += 1
        if coverage.get("degraded") and not (
            asset_type in {"cn_etf", "cn_fund", "cn_index"}
            and (
                coverage.get("directional_catalyst_hit")
                or int(coverage.get("theme_news_count") or 0) > 0
                or str(coverage.get("diagnosis", "")).strip() in {"theme_only_live", "confirmed_live"}
            )
        ):
            degraded_count += 1
    total = len(rows)
    degraded = (news_mode != "live" and theme_count == 0) or degraded_count > 0
    if theme_count > 0:
        note = "本轮直连新闻覆盖偏弱，但 ETF/基金主题级延续催化已有命中，不应机械地按零催化理解。"
    else:
        note = "本轮实时新闻/事件覆盖存在降级，名单更容易偏保守。" if degraded else "本轮新闻/事件覆盖基本正常。"
    return {
        "news_mode": news_mode,
        "degraded": degraded,
        "structured_rate": structured_count / total if total else 0.0,
        "direct_news_rate": direct_count / total if total else 0.0,
        "total": total,
        "note": note,
        "lines": [
            f"结构化事件覆盖 {structured_count}/{total}",
            f"高置信直接新闻覆盖 {direct_count}/{total}",
        ],
    }


def grade_pick_delivery(
    *,
    report_type: str,
    discovery_mode: str,
    coverage: Mapping[str, Any] | None,
    scan_pool: int,
    passed_pool: int,
    winner: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    def _observe_only_winner(payload: Mapping[str, Any] | None) -> bool:
        item = dict(payload or {})
        if not item:
            return False
        asset_type = str(item.get("asset_type", "")).strip()
        rating_rank = int(dict(item.get("rating") or {}).get("rank", 0) or 0)
        narrative = dict(item.get("narrative") or {})
        judgment = dict(narrative.get("judgment") or {})
        action = dict(item.get("action") or {})
        direction = str(action.get("direction", "")).strip()
        position = str(action.get("position", "")).strip()
        tokens = " ".join(
            str(value).strip()
            for value in (
                item.get("trade_state"),
                judgment.get("state"),
                judgment.get("direction"),
                action.get("direction"),
                action.get("position"),
                action.get("entry"),
                action.get("scaling_plan"),
            )
            if str(value).strip()
        )
        if not tokens:
            return False
        hard_observe_markers = ("观察为主", "回避", "暂不出手", "等待更好窗口", "先按观察仓", "仅观察仓")
        if (
            asset_type in {"cn_stock", "cn_etf", "cn_fund", "cn_index"}
            and rating_rank >= 3
            and direction in {"做多", "观望偏多", "观望"}
            and position
            and "暂不出手" not in position
        ):
            return False
        if any(marker in tokens for marker in hard_observe_markers):
            return True
        pullback_execution_markers = ("持有优于追高", "回调更优", "小仓试仓", "分批", "做多")
        if (
            asset_type in {"cn_stock", "cn_etf", "cn_fund", "cn_index"}
            and rating_rank >= 3
            and any(marker in tokens for marker in pullback_execution_markers)
        ):
            return False
        return "观察" in tokens

    def _winner_preserves_recommendation(payload: Mapping[str, Any] | None) -> bool:
        item = dict(payload or {})
        if not item:
            return False
        dimensions = dict(item.get("dimensions") or {})
        asset_type = str(item.get("asset_type", "")).strip()
        catalyst = dict(dimensions.get("catalyst") or {})
        coverage = dict(catalyst.get("coverage") or {})
        rating_rank = int(dict(item.get("rating") or {}).get("rank", 0) or 0)
        catalyst_score = float(catalyst.get("score") or 0.0)
        technical_score = float(dict(dimensions.get("technical") or {}).get("score") or 0.0)
        fundamental_score = float(dict(dimensions.get("fundamental") or {}).get("score") or 0.0)
        relative_score = float(dict(dimensions.get("relative_strength") or {}).get("score") or 0.0)
        risk_score = float(dict(dimensions.get("risk") or {}).get("score") or 0.0)
        has_structured_signal = bool(
            coverage.get("effective_structured_event")
            or coverage.get("structured_event")
            or coverage.get("forward_event")
        )
        has_theme_signal = bool(
            coverage.get("directional_catalyst_hit")
            or int(coverage.get("theme_news_count") or 0) > 0
            or int(coverage.get("recent_theme_news_pool_count") or 0) > 0
        )
        has_news_signal = bool(
            coverage.get("high_confidence_company_news")
            or int(coverage.get("direct_news_count") or 0) > 0
            or (
                asset_type not in {"cn_etf", "cn_fund", "cn_index"}
                and int(coverage.get("news_pool_count") or 0) > 0
            )
        )
        theme_only_signal = (
            asset_type in {"cn_etf", "cn_fund", "cn_index"}
            and has_theme_signal
            and not has_structured_signal
            and not has_news_signal
        )
        etf_theme_continuation = (
            asset_type in {"cn_etf", "cn_fund", "cn_index"}
            and rating_rank >= 3
            and not bool(coverage.get("degraded"))
            and not theme_only_signal
            and fundamental_score >= 40
            and relative_score >= 30
            and technical_score >= 20
            and (catalyst_score >= 15 or risk_score >= 50)
        )
        stock_resilient_setup = (
            asset_type == "cn_stock"
            and rating_rank >= 3
            and fundamental_score >= 65
            and relative_score >= 20
            and technical_score >= 15
        )
        if asset_type in {"cn_etf", "cn_fund", "cn_index"} and bool(coverage.get("degraded")) and theme_only_signal:
            return False
        return rating_rank >= 2 and not _observe_only_winner(item) and (
            catalyst_score >= 25
            or has_structured_signal
            or has_news_signal
            or (asset_type in {"cn_etf", "cn_fund", "cn_index"} and has_theme_signal and not bool(coverage.get("degraded")))
            or etf_theme_continuation
            or stock_resilient_setup
        )

    coverage_payload = dict(coverage or {})
    degraded = bool(coverage_payload.get("degraded"))
    winner_resilient = _winner_preserves_recommendation(winner)
    winner_payload = dict(winner or {})
    winner_asset_type = str(winner_payload.get("asset_type", "")).strip()
    winner_catalyst_coverage = dict(dict(dict(winner_payload.get("dimensions") or {}).get("catalyst") or {}).get("coverage") or {})
    winner_has_direct_or_structured = bool(
        winner_catalyst_coverage.get("effective_structured_event")
        or winner_catalyst_coverage.get("structured_event")
        or winner_catalyst_coverage.get("forward_event")
        or winner_catalyst_coverage.get("high_confidence_company_news")
        or int(winner_catalyst_coverage.get("direct_news_count") or 0) > 0
    )
    if degraded and winner_asset_type in {"cn_etf", "cn_fund", "cn_index"} and not winner_has_direct_or_structured:
        winner_resilient = False
    total = int(coverage_payload.get("total") or 0)
    structured_rate = float(coverage_payload.get("structured_rate") or 0.0)
    direct_rate = float(coverage_payload.get("direct_news_rate") or 0.0)
    summary_only = (
        report_type in {"etf_pick", "fund_pick"}
        and degraded
        and not winner_resilient
        and (total <= 1 or (structured_rate < 0.34 and direct_rate < 0.34))
    )
    label = "标准推荐稿"
    code = "standard_recommendation"
    notes = [
        f"当前流程不是把全市场每只标的都做完整八维深扫，而是先初筛 `{scan_pool}` 只，再对其中 `{passed_pool}` 只做完整分析。"
    ]
    discovery_mode = str(discovery_mode or "")

    if discovery_mode == "default_candidates_fallback":
        code = "fallback_watch_only"
        label = "兜底观察稿"
        notes.append("全市场初筛没有形成稳定候选，本次已回退到默认候选池，只适合当作兜底观察名单。")
    elif discovery_mode == "realtime_universe":
        code = "realtime_snapshot_note"
        label = "盘中快照成稿"
        notes.append("当前全市场初筛基于盘中实时/缓存快照，不是 Tushare 日终正式快照；即使进入正式交付链，数据口径仍应按盘中快照理解。")
    elif discovery_mode == "watchlist_fallback":
        code = "proxy_watch_only"
        label = "代理观察稿"
        notes.append("当前扫描池回退到了 watchlist/代理池，范围不是完整全市场，不应按正式推荐理解。")
    elif discovery_mode == "mixed_pool":
        code = "mixed_pool_observe"
        label = "混合池观察稿"
        notes.append("当前候选池混入了代理/watchlist 标的，覆盖范围不是纯全市场模式，更适合按观察优先处理。")
    elif discovery_mode == "manual_candidates":
        code = "manual_scope_note"
        label = "定向候选稿"
        notes.append("本次是手动候选范围内的相对比较，不代表完整全市场优选结论。")
    elif degraded and not winner_resilient:
        code = "degraded_observation"
        label = "降级观察稿"
        notes.append("新闻/事件覆盖存在降级，本次更适合作为观察优先对象，不宜当成强执行型推荐。")
    elif degraded:
        notes.append("新闻/事件覆盖存在局部降级，但当前优先标的自身仍有可执行证据，本次仍按标准推荐稿处理。")
    elif report_type in {"etf_pick", "fund_pick"} and _observe_only_winner(winner):
        code = "observe_priority_note"
        label = "观察优先稿"
        notes.append("当前排第一的标的仍是观察/持有优先口径，方向可以继续跟，但不应按正式申赎/交易推荐理解。")
    if summary_only:
        notes.append("当前覆盖率过低，本次只输出摘要观察稿，不展开完整动作模板。")

    if coverage_payload.get("total") and coverage_payload.get("total", 0) < max(2, passed_pool):
        notes.append("可统计覆盖样本比进入完整分析的样本更少，说明部分候选仍有数据缺口。")

    observe_only = code != "standard_recommendation"
    state_line = (
        "今天先给一个观察优先对象，不按正式买入稿理解。"
        if observe_only
        else "这份稿件当前只代表本轮排序里的单只重点对象，具体执行口径仍以交付等级和首页执行卡为准。"
    )
    notes.append(state_line)
    return {
        "code": code,
        "label": label,
        "observe_only": observe_only,
        "summary_only": summary_only,
        "notes": notes,
    }


def enrich_pick_payload_with_score_history(
    payload: Dict[str, Any],
    *,
    scope: str,
    snapshot_path: Path,
    model_version: str,
    model_changelog: Sequence[str],
    rank_key: Callable[[Mapping[str, Any]], Any],
) -> Dict[str, Any]:
    history_store = _load_history_store(snapshot_path)
    scope_history = _normalize_scope_history(history_store.get(scope) or {})
    current_snapshot = _build_snapshot(payload.get("top", []) or [], str(payload.get("generated_at", "")), model_version)
    current_date = _snapshot_date(str(payload.get("generated_at", "")))
    baseline_scope = dict((scope_history.get("daily_baselines") or {}).get(current_date) or {})
    comparison_scope = baseline_scope if baseline_scope else {}
    comparison_items = dict(comparison_scope.get("items") or {})
    previous_scope = _previous_scope_snapshot(scope_history, current_date)
    previous_items = dict(previous_scope.get("items") or {})
    comparison_basis_label = "当日基准版" if comparison_scope else ""
    comparison_basis_at = str(comparison_scope.get("generated_at", "")) if comparison_scope else ""
    comparison_model_version = str(comparison_scope.get("model_version", "")) if comparison_scope else ""
    model_version_warning = ""
    if comparison_model_version and comparison_model_version != model_version:
        model_version_warning = f"当前 `{model_version}`，基准版 `{comparison_model_version}`，两次运行的规则口径并不完全相同。"

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

    payload["top"] = sorted(all_items, key=rank_key, reverse=True)[: len(all_items)]
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

    payload["model_version"] = model_version
    payload["comparison_basis_label"] = comparison_basis_label
    payload["comparison_basis_at"] = comparison_basis_at
    payload["comparison_model_version"] = comparison_model_version
    payload["previous_snapshot_at"] = comparison_basis_at
    payload["baseline_snapshot_at"] = str(baseline_scope.get("generated_at", "")) if baseline_scope else str(current_snapshot.get("generated_at", ""))
    payload["baseline_model_version"] = str(baseline_scope.get("model_version", "")) if baseline_scope else model_version
    payload["is_daily_baseline"] = not bool(baseline_scope)
    payload["model_version_warning"] = model_version_warning
    payload["model_changelog"] = list(model_changelog)
    coverage_rows = list(payload.get("coverage_analyses", []) or payload.get("top", []) or [])
    payload["pick_coverage"] = summarize_pick_coverage(coverage_rows)

    daily_baselines = dict(scope_history.get("daily_baselines") or {})
    if current_date and not baseline_scope:
        daily_baselines[current_date] = current_snapshot
    history_store[scope] = {
        "latest": current_snapshot,
        "daily_baselines": daily_baselines,
    }
    save_json(snapshot_path, history_store)
    return payload


def _load_history_store(snapshot_path: Path) -> Dict[str, Any]:
    try:
        payload = load_json(snapshot_path, default={}) or {}
    except json.JSONDecodeError:
        if snapshot_path.exists():
            quarantine = snapshot_path.with_name(
                f"{snapshot_path.stem}.corrupt.{datetime.now().strftime('%Y%m%d_%H%M%S')}{snapshot_path.suffix}"
            )
            snapshot_path.replace(quarantine)
        return {}
    return dict(payload)


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
    factors: Dict[str, Dict[str, Any]] = {}
    for item in dimension.get("factors", []) or []:
        name = str(item.get("name", "")).strip()
        if not name:
            continue
        factor_meta = dict(item.get("factor_meta") or {})
        factors[name] = {
            "display_score": str(item.get("display_score", "")),
            "signal": str(item.get("signal", "")),
            "factor_id": str(item.get("factor_id", "")),
            "family": str(factor_meta.get("family", "")),
            "state": str(factor_meta.get("state", "")),
            "visibility_class": str(factor_meta.get("visibility_class", "")),
            "proxy_level": str(factor_meta.get("proxy_level", "")),
            "supports_strategy_candidate": bool(factor_meta.get("supports_strategy_candidate")),
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


def _build_snapshot(top: Sequence[Mapping[str, Any]], generated_at: str, model_version: str) -> Dict[str, Any]:
    return {
        "generated_at": generated_at,
        "model_version": model_version,
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
    if prev_score is None or curr_score is None:
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
    analysis["rating"] = _rating_from_dimensions(
        analysis["dimensions"],
        analysis.get("rating", {}).get("warnings", []) or [],
        asset_type=str(analysis.get("asset_type", "") or ""),
    )
