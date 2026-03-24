"""Strategy v1 prediction-ledger helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import pandas as pd

from src.processors.factor_meta import summarize_factor_contracts_from_analysis
from src.processors.opportunity_engine import analyze_opportunity, build_market_context
from src.processors.provenance import history_as_of
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.utils.config import detect_asset_type
from src.utils.market import compute_history_metrics, fetch_asset_history


STRATEGY_V1_UNIVERSE = "a_share_liquid_stock_v1"
STRATEGY_V1_TARGET = "20d_excess_return_vs_csi800_rank"
STRATEGY_V1_BENCHMARK_SYMBOL = "000906.SH"
STRATEGY_V1_BENCHMARK_NAME = "中证800"
STRATEGY_V1_FACTOR_VERSION = "strategy_v1_seed_eight_dimension_2026-03-14"
STRATEGY_V1_REPLAY_FACTOR_VERSION = "strategy_v1_replay_price_only_2026-03-14"
STRATEGY_V1_DIRECTIONAL_COST_BPS = 0.005
STRATEGY_V1_NEUTRAL_BAND = 0.02
STRATEGY_V1_ASSET_GAP_DAYS = 20
STRATEGY_V1_REPLAY_WEIGHT_SCHEME = {
    "medium_term_momentum": 0.28,
    "benchmark_relative": 0.30,
    "technical_confirmation": 0.20,
    "risk_efficiency": 0.17,
    "liquidity_profile": 0.05,
}
STRATEGY_V1_WEIGHT_SCHEME = {
    "technical": 0.22,
    "relative_strength": 0.20,
    "catalyst": 0.16,
    "fundamental": 0.14,
    "risk": 0.12,
    "macro": 0.08,
    "seasonality": 0.05,
    "chips": 0.03,
}
STRATEGY_V1_EXPERIMENT_VARIANTS = {
    "baseline": {
        "label": "baseline",
        "hypothesis": "沿用当前 replay v1 默认权重，作为所有 challenger 的基线。",
        "weight_scheme": STRATEGY_V1_REPLAY_WEIGHT_SCHEME,
    },
    "momentum_tilt": {
        "label": "momentum_tilt",
        "hypothesis": "提高中周期动量和相对基准强弱权重，测试更激进的趋势跟随。",
        "weight_scheme": {
            "medium_term_momentum": 0.34,
            "benchmark_relative": 0.31,
            "technical_confirmation": 0.20,
            "risk_efficiency": 0.10,
            "liquidity_profile": 0.05,
        },
    },
    "defensive_tilt": {
        "label": "defensive_tilt",
        "hypothesis": "提高风险效率和流动性约束权重，测试更保守的防守框架。",
        "weight_scheme": {
            "medium_term_momentum": 0.20,
            "benchmark_relative": 0.24,
            "technical_confirmation": 0.18,
            "risk_efficiency": 0.28,
            "liquidity_profile": 0.10,
        },
    },
    "confirmation_tilt": {
        "label": "confirmation_tilt",
        "hypothesis": "提高技术确认权重，避免只有相对强弱而缺乏确认时过早出手。",
        "weight_scheme": {
            "medium_term_momentum": 0.24,
            "benchmark_relative": 0.26,
            "technical_confirmation": 0.30,
            "risk_efficiency": 0.15,
            "liquidity_profile": 0.05,
        },
    },
}
STRATEGY_V1_PROMOTION_MIN_VALIDATED_ROWS = 6
STRATEGY_V1_PROMOTION_MIN_PRIMARY_SCORE_DELTA = 0.5
STRATEGY_V1_PROMOTION_MIN_AVG_EXCESS_RETURN_DELTA = 0.002
STRATEGY_V1_PROMOTION_MIN_AVG_NET_DIRECTIONAL_RETURN_DELTA = 0.002
STRATEGY_V1_PROMOTION_MAX_DRAWDOWN_REGRESSION = -0.02
STRATEGY_V1_ROLLBACK_MIN_VALIDATED_ROWS = 6
STRATEGY_V1_ROLLBACK_WATCH_HIT_RATE = 0.45
STRATEGY_V1_ROLLBACK_TRIGGER_HIT_RATE = 0.35
STRATEGY_V1_ROLLBACK_TRIGGER_AVG_EXCESS_RETURN = -0.02
STRATEGY_V1_ROLLBACK_TRIGGER_AVG_NET_DIRECTIONAL_RETURN = -0.01
STRATEGY_V1_ROLLBACK_WATCH_STRUCTURAL_MISS_SHARE = 0.34
STRATEGY_V1_ROLLBACK_TRIGGER_STRUCTURAL_MISS_SHARE = 0.5
STRATEGY_V1_OUT_OF_SAMPLE_MIN_VALIDATED_ROWS = 6
STRATEGY_V1_OUT_OF_SAMPLE_MIN_DEVELOPMENT_ROWS = 4
STRATEGY_V1_OUT_OF_SAMPLE_MIN_HOLDOUT_ROWS = 2
STRATEGY_V1_OUT_OF_SAMPLE_MAX_HIT_RATE_REGRESSION = -0.15
STRATEGY_V1_OUT_OF_SAMPLE_MAX_AVG_EXCESS_REGRESSION = -0.02
STRATEGY_V1_OUT_OF_SAMPLE_MAX_AVG_NET_REGRESSION = -0.015
STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORT_SYMBOLS = 3
STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORTS = 3
STRATEGY_V1_CROSS_SECTIONAL_MIN_AVG_RANK_CORR = 0.05
STRATEGY_V1_CROSS_SECTIONAL_MIN_AVG_TOP_BOTTOM_SPREAD = 0.01

_DIMENSION_LABELS = {
    "technical": "技术/趋势",
    "relative_strength": "相对强弱",
    "catalyst": "催化/事件",
    "fundamental": "基本面",
    "risk": "风险收益",
    "macro": "宏观",
    "seasonality": "季节/日历",
    "chips": "筹码结构",
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    if pd.isna(number):
        return default
    return number


def _dimension_score_map(dimensions: Mapping[str, Any]) -> Dict[str, float]:
    score_map: Dict[str, float] = {}
    for key in STRATEGY_V1_WEIGHT_SCHEME:
        payload = dict(dimensions.get(key) or {})
        score_map[key] = _safe_float(payload.get("score"), 50.0)
    return score_map


def _seed_rank_score(score_map: Mapping[str, float]) -> float:
    weighted = 0.0
    total_weight = 0.0
    for key, weight in STRATEGY_V1_WEIGHT_SCHEME.items():
        score = _safe_float(score_map.get(key), 50.0)
        weighted += score * float(weight)
        total_weight += float(weight)
    if total_weight <= 0:
        return 50.0
    return round(weighted / total_weight, 2)


def _confidence_payload(seed_score: float) -> Dict[str, Any]:
    distance = abs(float(seed_score) - 50.0)
    normalized = min(distance / 30.0, 1.0)
    if distance >= 18:
        bucket = "high"
        label = "高"
    elif distance >= 8:
        bucket = "medium"
        label = "中"
    else:
        bucket = "low"
        label = "低"
    return {
        "score": round(normalized, 3),
        "bucket": bucket,
        "label": label,
    }


def _prediction_value(seed_score: float) -> Dict[str, Any]:
    score = float(seed_score)
    if score >= 75:
        return {
            "expected_excess_direction": "positive",
            "expected_rank_bucket": "top_decile_candidate",
            "summary": "更像 20 日超额收益的强势上层候选。",
        }
    if score >= 65:
        return {
            "expected_excess_direction": "positive",
            "expected_rank_bucket": "upper_quintile_candidate",
            "summary": "更像 20 日超额收益的上层候选，但还没到最强 decile 信号。",
        }
    if score >= 55:
        return {
            "expected_excess_direction": "positive",
            "expected_rank_bucket": "upper_half_candidate",
            "summary": "更像跑赢基准的上半区候选，但把握还不算特别集中。",
        }
    if score >= 45:
        return {
            "expected_excess_direction": "neutral",
            "expected_rank_bucket": "middle_bucket",
            "summary": "当前更接近中性桶，信号还不足以说明明显跑赢或跑输。",
        }
    if score >= 35:
        return {
            "expected_excess_direction": "negative",
            "expected_rank_bucket": "lower_half_candidate",
            "summary": "更像 20 日相对收益偏弱的下半区候选。",
        }
    return {
        "expected_excess_direction": "negative",
        "expected_rank_bucket": "bottom_decile_candidate",
        "summary": "更像 20 日超额收益的明显弱势尾部候选。",
    }


def _candidate_effective_from(as_of: str) -> str:
    try:
        stamp = pd.Timestamp(as_of)
    except Exception:
        return ""
    if pd.isna(stamp):
        return ""
    return str((stamp + pd.offsets.BDay(1)).date())


def _median_turnover_60d(history: pd.DataFrame) -> float:
    normalized = _safe_normalize_history(history)
    amount = pd.to_numeric(normalized.get("amount"), errors="coerce")
    if amount is not None:
        amount_tail = amount.tail(60).dropna()
        if not amount_tail.empty and float(amount_tail.abs().sum()) > 0:
            return float(amount_tail.median())
    close = pd.to_numeric(normalized.get("close"), errors="coerce").fillna(0.0)
    volume = pd.to_numeric(normalized.get("volume"), errors="coerce").fillna(0.0)
    turnover = (close * volume).tail(60).dropna()
    if turnover.empty:
        return 0.0
    return float(turnover.median())


def _safe_normalize_history(history: Any) -> pd.DataFrame:
    try:
        return normalize_ohlcv_frame(history)
    except Exception:
        return pd.DataFrame()


def _safe_history_as_of(history: Any) -> str:
    normalized = _safe_normalize_history(history)
    if normalized.empty:
        return "—"
    return history_as_of(normalized)


def _history_window(history: Any) -> Dict[str, Any]:
    normalized = _safe_normalize_history(history)
    if normalized.empty:
        return {"rows": 0, "start": "—", "end": "—"}
    return {
        "rows": int(len(normalized)),
        "start": str(pd.Timestamp(normalized["date"].iloc[0]).date()),
        "end": str(pd.Timestamp(normalized["date"].iloc[-1]).date()),
    }


def _clamp(value: float, low: float = 0.0, high: float = 100.0) -> float:
    return float(max(low, min(high, value)))


def _scaled(value: float, band: float) -> float:
    if band <= 0:
        return 0.0
    return max(-1.0, min(1.0, float(value) / float(band)))


def _signal_value(signal: str) -> float:
    normalized = str(signal or "").strip().lower()
    if normalized in {"bullish", "bullish_trend", "golden_cross"}:
        return 1.0
    if normalized in {"bearish", "bearish_trend", "death_cross"}:
        return -1.0
    return 0.0


def _benchmark_fixture(
    asset_history: Any,
    benchmark_history: Any,
    *,
    as_of: Any = "",
    horizon_days: int = 20,
    required_rows: int = 250,
) -> Dict[str, Any]:
    asset_frame = _safe_normalize_history(asset_history)
    benchmark_frame = _safe_normalize_history(benchmark_history)
    asset_window = _history_window(asset_frame)
    benchmark_window = _history_window(benchmark_frame)
    overlap_rows = 0
    overlap_start = "—"
    overlap_end = "—"
    aligned_as_of = False
    asset_as_of = "—"
    benchmark_as_of = "—"
    as_of_gap_days = 0
    future_window_ready = False
    blockers: List[str] = []

    if benchmark_frame.empty:
        blockers.append("benchmark_missing")
    elif asset_frame.empty:
        blockers.append("asset_history_missing")
    else:
        asset_dates = pd.Index(pd.to_datetime(asset_frame["date"], errors="coerce").dropna())
        benchmark_dates = pd.Index(pd.to_datetime(benchmark_frame["date"], errors="coerce").dropna())
        overlap = asset_dates.intersection(benchmark_dates)
        overlap_rows = int(len(overlap))
        if overlap_rows > 0:
            overlap_start = str(pd.Timestamp(overlap[0]).date())
            overlap_end = str(pd.Timestamp(overlap[-1]).date())
        if overlap_rows < int(required_rows):
            blockers.append("benchmark_overlap_insufficient")

        target_as_of = as_of or asset_window["end"]
        asset_index = _locate_as_of_index(asset_frame, target_as_of)
        benchmark_index = _locate_as_of_index(benchmark_frame, target_as_of)
        if asset_index >= 0:
            asset_as_of = str(pd.Timestamp(asset_frame["date"].iloc[asset_index]).date())
        if benchmark_index >= 0:
            benchmark_as_of = str(pd.Timestamp(benchmark_frame["date"].iloc[benchmark_index]).date())
        aligned_as_of = asset_index >= 0 and benchmark_index >= 0 and asset_as_of == benchmark_as_of
        if asset_index >= 0 and benchmark_index >= 0 and not aligned_as_of:
            blockers.append("benchmark_as_of_misaligned")
            try:
                as_of_gap_days = abs((pd.Timestamp(asset_as_of) - pd.Timestamp(benchmark_as_of)).days)
            except Exception:
                as_of_gap_days = 0
        future_window_ready = (
            asset_index >= 0
            and benchmark_index >= 0
            and asset_index + int(horizon_days) < len(asset_frame)
            and benchmark_index + int(horizon_days) < len(benchmark_frame)
        )

    if "benchmark_missing" in blockers:
        status = "missing"
    elif blockers:
        status = "partial"
    else:
        status = "aligned"

    if status == "aligned":
        summary = (
            f"基准窗口已对齐：资产 `{asset_as_of}`，基准 `{benchmark_as_of}`，"
            f"重叠 `{overlap_rows}` 行，覆盖 `{overlap_start}` -> `{overlap_end}`。"
        )
    elif status == "missing":
        summary = "当前缺少可用 benchmark 历史，strategy v1 的相对收益合同无法完整成立。"
    else:
        blocker_text = " / ".join(blockers)
        summary = (
            f"benchmark fixture 不完整：资产 `{asset_as_of}`，基准 `{benchmark_as_of}`，"
            f"重叠 `{overlap_rows}` 行，主要问题 `{blocker_text}`。"
        )

    return {
        "status": status,
        "required_rows": int(required_rows),
        "asset_window": asset_window,
        "benchmark_window": benchmark_window,
        "overlap_rows": overlap_rows,
        "overlap_start": overlap_start,
        "overlap_end": overlap_end,
        "asset_as_of": asset_as_of,
        "benchmark_as_of": benchmark_as_of,
        "aligned_as_of": bool(aligned_as_of),
        "as_of_gap_days": int(as_of_gap_days),
        "future_window_ready": bool(future_window_ready),
        "blockers": blockers,
        "summary": summary,
    }


def _aggregate_benchmark_fixtures(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    fixtures = [dict(row.get("benchmark_fixture") or {}) for row in rows if dict(row.get("benchmark_fixture") or {})]
    if not fixtures:
        return {}
    raw_status_counts: Dict[str, int] = {}
    for fixture in fixtures:
        status = str(fixture.get("status", "missing"))
        raw_status_counts[status] = raw_status_counts.get(status, 0) + 1
    status_counts = {
        status: raw_status_counts[status]
        for status in ("aligned", "partial", "missing")
        if raw_status_counts.get(status)
    }
    for status, count in raw_status_counts.items():
        if status not in status_counts:
            status_counts[status] = count
    overlap_values = [int(fixture.get("overlap_rows") or 0) for fixture in fixtures]
    gap_values = [int(fixture.get("as_of_gap_days") or 0) for fixture in fixtures]
    ready_values = [bool(fixture.get("future_window_ready")) for fixture in fixtures]
    required_rows = int(fixtures[0].get("required_rows") or 250)
    sample_count = len(fixtures)
    min_overlap_rows = min(overlap_values) if overlap_values else 0
    max_as_of_gap_days = max(gap_values) if gap_values else 0
    future_window_ready_count = sum(1 for ready in ready_values if ready)
    future_window_pending_count = sum(1 for ready in ready_values if not ready)
    aligned_count = int(status_counts.get("aligned") or 0)
    missing_count = int(status_counts.get("missing") or 0)
    if aligned_count == sample_count and max_as_of_gap_days == 0 and min_overlap_rows >= required_rows:
        summary = "当前样本的 benchmark 合同整体对齐，未见 overlap 或 as_of 缺口。"
    elif missing_count == sample_count:
        summary = "当前样本全部缺少可用 benchmark 历史，relative benchmark 合同尚未成立。"
    else:
        summary = "部分样本的 benchmark 合同不完整，需要结合状态分布、最小 overlap 和 as_of 偏差一起看。"
    return {
        "sample_count": sample_count,
        "status_counts": status_counts,
        "min_overlap_rows": min_overlap_rows,
        "max_as_of_gap_days": max_as_of_gap_days,
        "future_window_ready_count": future_window_ready_count,
        "future_window_pending_count": future_window_pending_count,
        "required_rows": required_rows,
        "summary": summary,
    }


def _lag_visibility_fixture(
    factor_contract: Mapping[str, Any] | None,
    *,
    mode: str = "prediction",
    required_strategy_candidate_ready: int = 1,
) -> Dict[str, Any]:
    contract = dict(factor_contract or {})
    readiness = dict(contract.get("fixture_readiness") or {})
    blockers = [dict(item) for item in list(contract.get("lag_visibility_blockers") or []) if dict(item)]
    total_factors = int(readiness.get("total_factors") or contract.get("registered_factor_rows") or 0)
    lag_ready_count = int(readiness.get("lag_ready_count") or 0)
    lag_blocked_count = int(readiness.get("lag_blocked_count") or 0)
    visibility_ready_count = int(readiness.get("visibility_ready_count") or 0)
    visibility_blocked_count = int(readiness.get("visibility_blocked_count") or 0)
    point_in_time_ready_count = int(readiness.get("point_in_time_ready_count") or 0)
    point_in_time_blocked_count = int(readiness.get("point_in_time_blocked_count") or len(blockers))
    strategy_candidate_total = int(readiness.get("strategy_candidate_total") or 0)
    strategy_candidate_ready_count = int(readiness.get("strategy_candidate_ready_count") or len(list(contract.get("strategy_candidate_factor_ids") or [])))
    degraded_count = int(readiness.get("degraded_count") or len(list(contract.get("degraded_factor_ids") or [])))
    max_lag_days = int(readiness.get("max_lag_days") or 0)
    blocker_factor_ids = [str(item.get("factor_id", "")).strip() for item in blockers if str(item.get("factor_id", "")).strip()]
    strategy_blocker_factor_ids = [
        str(item.get("factor_id", "")).strip()
        for item in blockers
        if bool(item.get("supports_strategy_candidate")) and str(item.get("factor_id", "")).strip()
    ]

    if mode == "replay_price_only_v1":
        status = "not_applicable"
        summary = "历史 replay v1 当前只比较 price-only 因子，不走共享 factor_meta 的 lag / visibility fixture。"
    elif total_factors <= 0:
        status = "missing"
        summary = "当前没有拿到可复核的 factor meta 样本，lag / visibility fixture 无法成立。"
    elif strategy_candidate_total > 0 and strategy_candidate_ready_count < max(int(required_strategy_candidate_ready), 1):
        status = "blocked"
        summary = (
            f"当前 `{strategy_candidate_total}` 个 strategy candidate 因子里可 point-in-time 使用的只有 "
            f"`{strategy_candidate_ready_count}` 个，低于主合同要求。"
        )
    elif point_in_time_blocked_count > 0:
        status = "partial"
        summary = (
            f"当前已就绪 `{strategy_candidate_ready_count}/{strategy_candidate_total}` 个 strategy candidate，"
            f"但仍有 `{point_in_time_blocked_count}` 个因子没完成 lag / visibility fixture。"
        )
    else:
        status = "ready"
        summary = (
            f"当前 `{total_factors}` 个已注册因子都完成了 lag / visibility fixture，"
            f"其中 strategy candidate `{strategy_candidate_ready_count}/{strategy_candidate_total}`。"
        )

    return {
        "status": status,
        "mode": mode,
        "required_strategy_candidate_ready": max(int(required_strategy_candidate_ready), 1),
        "total_factors": total_factors,
        "lag_ready_count": lag_ready_count,
        "lag_blocked_count": lag_blocked_count,
        "visibility_ready_count": visibility_ready_count,
        "visibility_blocked_count": visibility_blocked_count,
        "point_in_time_ready_count": point_in_time_ready_count,
        "point_in_time_blocked_count": point_in_time_blocked_count,
        "strategy_candidate_total": strategy_candidate_total,
        "strategy_candidate_ready_count": strategy_candidate_ready_count,
        "degraded_count": degraded_count,
        "max_lag_days": max_lag_days,
        "blocker_factor_ids": blocker_factor_ids,
        "strategy_blocker_factor_ids": strategy_blocker_factor_ids,
        "blockers": blockers,
        "summary": summary,
    }


def _aggregate_lag_visibility_fixtures(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    fixtures = [dict(row.get("lag_visibility_fixture") or {}) for row in rows if dict(row.get("lag_visibility_fixture") or {})]
    if not fixtures:
        return {}
    raw_status_counts: Dict[str, int] = {}
    for fixture in fixtures:
        status = str(fixture.get("status", "missing"))
        raw_status_counts[status] = raw_status_counts.get(status, 0) + 1
    status_counts = {
        status: raw_status_counts[status]
        for status in ("ready", "partial", "blocked", "missing", "not_applicable")
        if raw_status_counts.get(status)
    }
    for status, count in raw_status_counts.items():
        if status not in status_counts:
            status_counts[status] = count
    sample_count = len(fixtures)
    min_strategy_candidate_ready_count = min(int(fixture.get("strategy_candidate_ready_count") or 0) for fixture in fixtures)
    max_point_in_time_blocked_count = max(int(fixture.get("point_in_time_blocked_count") or 0) for fixture in fixtures)
    max_lag_days = max(int(fixture.get("max_lag_days") or 0) for fixture in fixtures)
    ready_count = int(status_counts.get("ready") or 0)
    blocked_count = int(status_counts.get("blocked") or 0)
    not_applicable_count = int(status_counts.get("not_applicable") or 0)
    if not_applicable_count == sample_count:
        summary = "当前样本全部走 price-only replay，lag / visibility fixture 对共享因子池暂不适用。"
    elif ready_count == sample_count:
        summary = "当前样本的 lag / visibility fixture 全部就绪，未见 point-in-time blocker。"
    elif blocked_count > 0:
        summary = "部分样本缺少可用的 strategy candidate point-in-time 因子，lag / visibility fixture 当前会阻断主预测。"
    else:
        summary = "部分样本的 lag / visibility fixture 仍不完整，但还没有把主合同完全阻断。"
    return {
        "sample_count": sample_count,
        "status_counts": status_counts,
        "min_strategy_candidate_ready_count": min_strategy_candidate_ready_count,
        "max_point_in_time_blocked_count": max_point_in_time_blocked_count,
        "max_lag_days": max_lag_days,
        "summary": summary,
    }


def _project_window_end(history: pd.DataFrame, as_of: Any, horizon_days: int) -> str:
    normalized = _safe_normalize_history(history)
    try:
        stamp = pd.Timestamp(as_of)
    except Exception:
        return "—"
    if normalized.empty:
        return str((stamp + pd.offsets.BDay(max(int(horizon_days), 1))).date())
    index = _locate_as_of_index(normalized, stamp)
    if index >= 0 and index + int(horizon_days) < len(normalized):
        return str(pd.Timestamp(normalized["date"].iloc[index + int(horizon_days)]).date())
    anchor = stamp if index < 0 else pd.Timestamp(normalized["date"].iloc[index])
    return str((anchor + pd.offsets.BDay(max(int(horizon_days), 1))).date())


def _trading_gap_days(history: pd.DataFrame, earlier_as_of: Any, later_as_of: Any) -> Tuple[int, str]:
    normalized = _safe_normalize_history(history)
    try:
        earlier = pd.Timestamp(earlier_as_of)
        later = pd.Timestamp(later_as_of)
    except Exception:
        return 0, "unknown"
    if later < earlier:
        return 0, "unknown"
    if not normalized.empty:
        earlier_index = _locate_as_of_index(normalized, earlier)
        later_index = _locate_as_of_index(normalized, later)
        if earlier_index >= 0 and later_index >= 0 and later_index >= earlier_index:
            return later_index - earlier_index, "asset_history"
    return max(len(pd.bdate_range(earlier, later)) - 1, 0), "business_day_estimate"


def _required_overlap_gap_days(row: Mapping[str, Any]) -> Dict[str, int]:
    horizon_days = max(int(dict(row.get("horizon") or {}).get("days", 20) or 20), 1)
    cohort_contract = dict(row.get("cohort_contract") or {})
    configured_gap_days = max(int(cohort_contract.get("asset_reentry_gap_days") or 0), 0)
    required_gap_days = max(horizon_days, configured_gap_days or horizon_days)
    return {
        "horizon_days": horizon_days,
        "configured_gap_days": configured_gap_days,
        "required_gap_days": required_gap_days,
    }


def _augment_downgrade_flags(row: Dict[str, Any], *flags: str) -> Dict[str, Any]:
    current_flags = [str(item) for item in list(row.get("downgrade_flags") or []) if str(item).strip()]
    for flag in flags:
        text = str(flag).strip()
        if text and text not in current_flags:
            current_flags.append(text)
    row["downgrade_flags"] = current_flags
    return row


def _attach_overlap_fixtures(
    rows: Sequence[Mapping[str, Any]],
    history_loader,
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    prepared = [(index, dict(row)) for index, row in enumerate(rows)]
    sorted_rows = sorted(
        prepared,
        key=lambda item: (
            str(item[1].get("symbol", "")),
            str(item[1].get("as_of", "")),
            str(item[1].get("created_at", "")),
            str(item[1].get("prediction_id", "")),
        ),
    )
    previous_by_symbol: Dict[str, Dict[str, Any]] = {}
    updated_by_index: Dict[int, Dict[str, Any]] = {}

    for original_index, row in sorted_rows:
        symbol = str(row.get("symbol", "")).strip()
        asset_type = str(row.get("asset_type", "")).strip()
        as_of = str(row.get("as_of", "")).strip()
        history = _safe_normalize_history(history_loader(symbol, asset_type)) if symbol and asset_type else pd.DataFrame()
        gap_payload = _required_overlap_gap_days(row)
        horizon_days = int(gap_payload["horizon_days"])
        configured_gap_days = int(gap_payload["configured_gap_days"])
        required_gap_days = int(gap_payload["required_gap_days"])
        overlap_policy = str(dict(row.get("cohort_contract") or {}).get("overlap_policy", "")).strip() or "unspecified"
        previous = previous_by_symbol.get(symbol)
        window_end = _project_window_end(history, as_of, horizon_days) if as_of else "—"
        gap_trading_days = 0
        gap_method = "not_applicable"
        previous_as_of = ""
        previous_window_end = ""
        overlaps_previous_primary_window = False
        if previous:
            previous_as_of = str(previous.get("as_of", "")).strip()
            previous_window_end = str(previous.get("window_end", "")).strip()
            gap_trading_days, gap_method = _trading_gap_days(history, previous_as_of, as_of)
            overlaps_previous_primary_window = gap_trading_days < required_gap_days
        status = "blocked" if previous and overlaps_previous_primary_window else "ready"
        if not as_of:
            status = "missing"
        if previous and status == "blocked":
            summary = (
                f"当前样本距离上一主样本 `{previous_as_of}` 只有 `{gap_trading_days}` 个交易日，"
                f"低于 non-overlap 合同要求的 `{required_gap_days}` 个交易日。"
            )
        elif previous:
            summary = (
                f"当前样本距离上一主样本 `{previous_as_of}` 已有 `{gap_trading_days}` 个交易日，"
                f"满足 `{required_gap_days}` 日 non-overlap 合同。"
            )
        elif as_of:
            summary = "当前批次里还没有更早的同标的主样本，这条记录先作为 overlap anchor。"
        else:
            summary = "当前缺少可比较的 as_of，无法建立 overlap fixture。"
        overlap_fixture = {
            "status": status,
            "comparison_scope": "current_batch_same_symbol",
            "symbol": symbol,
            "as_of": as_of or "—",
            "window_start": as_of or "—",
            "window_end": window_end,
            "horizon_days": horizon_days,
            "configured_gap_days": configured_gap_days,
            "required_gap_days": required_gap_days,
            "overlap_policy": overlap_policy,
            "previous_sample_as_of": previous_as_of or "—",
            "previous_window_end": previous_window_end or "—",
            "gap_trading_days": int(gap_trading_days),
            "gap_method": gap_method,
            "overlaps_previous_primary_window": bool(overlaps_previous_primary_window),
            "summary": summary,
        }
        row["overlap_fixture"] = overlap_fixture
        if status == "blocked":
            _augment_downgrade_flags(row, "overlap_fixture_blocked")
        updated_by_index[original_index] = row
        previous_by_symbol[symbol] = {
            "as_of": as_of,
            "window_end": window_end,
        }

    updated_rows = [updated_by_index[index] for index in range(len(prepared))]
    fixtures = [dict(row.get("overlap_fixture") or {}) for row in updated_rows if dict(row.get("overlap_fixture") or {})]
    if not fixtures:
        return updated_rows, {}
    raw_status_counts: Dict[str, int] = {}
    for fixture in fixtures:
        status = str(fixture.get("status", "missing"))
        raw_status_counts[status] = raw_status_counts.get(status, 0) + 1
    status_counts = {
        status: raw_status_counts[status]
        for status in ("ready", "blocked", "missing")
        if raw_status_counts.get(status)
    }
    for status, count in raw_status_counts.items():
        if status not in status_counts:
            status_counts[status] = count
    compared_fixtures = [fixture for fixture in fixtures if str(fixture.get("previous_sample_as_of", "—")) != "—"]
    gap_values = [int(fixture.get("gap_trading_days") or 0) for fixture in compared_fixtures]
    required_gap_values = [int(fixture.get("required_gap_days") or 0) for fixture in fixtures]
    violation_rows = [
        {
            "symbol": str(fixture.get("symbol", "")),
            "as_of": str(fixture.get("as_of", "—")),
            "previous_sample_as_of": str(fixture.get("previous_sample_as_of", "—")),
            "gap_trading_days": int(fixture.get("gap_trading_days") or 0),
            "required_gap_days": int(fixture.get("required_gap_days") or 0),
        }
        for fixture in fixtures
        if bool(fixture.get("overlaps_previous_primary_window"))
    ]
    if int(status_counts.get("blocked") or 0) > 0:
        summary = "当前样本里存在 primary window 重叠，不能把这批 replay / validate 结果直接当成 non-overlap 主样本验证。"
    elif compared_fixtures:
        summary = "当前样本的 primary windows 没有重叠，已满足单标的 non-overlap 合同。"
    else:
        summary = "当前样本数不足以形成 overlap 比较，第一条记录只作为 anchor sample。"
    return updated_rows, {
        "sample_count": len(fixtures),
        "status_counts": status_counts,
        "compared_rows": len(compared_fixtures),
        "violation_count": len(violation_rows),
        "min_gap_trading_days": min(gap_values) if gap_values else 0,
        "max_required_gap_days": max(required_gap_values) if required_gap_values else 0,
        "summary": summary,
        "violation_rows": violation_rows,
    }


def _aggregate_overlap_fixtures(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    fixtures = [dict(row.get("overlap_fixture") or {}) for row in rows if dict(row.get("overlap_fixture") or {})]
    if not fixtures:
        return {}
    raw_status_counts: Dict[str, int] = {}
    for fixture in fixtures:
        status = str(fixture.get("status", "missing"))
        raw_status_counts[status] = raw_status_counts.get(status, 0) + 1
    status_counts = {
        status: raw_status_counts[status]
        for status in ("ready", "blocked", "missing")
        if raw_status_counts.get(status)
    }
    for status, count in raw_status_counts.items():
        if status not in status_counts:
            status_counts[status] = count
    compared_fixtures = [fixture for fixture in fixtures if str(fixture.get("previous_sample_as_of", "—")) != "—"]
    gap_values = [int(fixture.get("gap_trading_days") or 0) for fixture in compared_fixtures]
    required_gap_values = [int(fixture.get("required_gap_days") or 0) for fixture in fixtures]
    violation_rows = [
        {
            "symbol": str(fixture.get("symbol", "")),
            "as_of": str(fixture.get("as_of", "—")),
            "previous_sample_as_of": str(fixture.get("previous_sample_as_of", "—")),
            "gap_trading_days": int(fixture.get("gap_trading_days") or 0),
            "required_gap_days": int(fixture.get("required_gap_days") or 0),
        }
        for fixture in fixtures
        if bool(fixture.get("overlaps_previous_primary_window"))
    ]
    if int(status_counts.get("blocked") or 0) > 0:
        summary = "当前样本里存在 primary window 重叠，不能把这批 replay / validate 结果直接当成 non-overlap 主样本验证。"
    elif compared_fixtures:
        summary = "当前样本的 primary windows 没有重叠，已满足单标的 non-overlap 合同。"
    else:
        summary = "当前样本数不足以形成 overlap 比较，第一条记录只作为 anchor sample。"
    return {
        "sample_count": len(fixtures),
        "status_counts": status_counts,
        "compared_rows": len(compared_fixtures),
        "violation_count": len(violation_rows),
        "min_gap_trading_days": min(gap_values) if gap_values else 0,
        "max_required_gap_days": max(required_gap_values) if required_gap_values else 0,
        "summary": summary,
        "violation_rows": violation_rows,
    }


def _technical_snapshot(history: pd.DataFrame) -> Dict[str, Any]:
    analyzer = TechnicalAnalyzer(history)
    return analyzer.generate_scorecard({})


def _normalize_weight_scheme(weight_scheme: Mapping[str, Any]) -> Dict[str, float]:
    weights = {str(key): max(_safe_float(value), 0.0) for key, value in dict(weight_scheme or {}).items()}
    total = sum(weights.values())
    if total <= 0:
        weights = dict(STRATEGY_V1_REPLAY_WEIGHT_SCHEME)
        total = sum(weights.values())
    return {key: round(value / total, 6) for key, value in weights.items()}


def _replay_factor_engine(
    asset_history: pd.DataFrame,
    benchmark_history: pd.DataFrame,
    *,
    weight_scheme: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    metrics = compute_history_metrics(asset_history)
    benchmark_metrics = compute_history_metrics(benchmark_history) if not benchmark_history.empty else {}
    technical = _technical_snapshot(asset_history)
    ma_signal = str(dict(technical.get("ma_system") or {}).get("signal", "")).lower()
    macd_signal = str(dict(technical.get("macd") or {}).get("signal", "")).lower()
    rsi = _safe_float(dict(technical.get("rsi") or {}).get("RSI"), 50.0)
    volume_ratio = _safe_float(dict(technical.get("volume") or {}).get("vol_ratio"), 1.0)
    return_20d = _safe_float(metrics.get("return_20d"))
    return_60d = _safe_float(metrics.get("return_60d"))
    benchmark_return_20d = _safe_float(benchmark_metrics.get("return_20d"))
    benchmark_return_60d = _safe_float(benchmark_metrics.get("return_60d"))
    relative_20d = return_20d - benchmark_return_20d
    relative_60d = return_60d - benchmark_return_60d
    price_percentile = _safe_float(metrics.get("price_percentile_1y"), 0.5)
    vol_20d = _safe_float(metrics.get("volatility_20d"))
    max_drawdown = abs(_safe_float(metrics.get("max_drawdown_1y")))
    median_turnover_60d = _median_turnover_60d(asset_history)

    medium_term_momentum = _clamp(
        50.0
        + _scaled(return_20d, 0.15) * 16.0
        + _scaled(return_60d, 0.30) * 12.0
        + _scaled(price_percentile - 0.5, 0.35) * 6.0
    )
    benchmark_relative = _clamp(
        50.0
        + _scaled(relative_20d, 0.12) * 20.0
        + _scaled(relative_60d, 0.20) * 10.0
    )
    technical_confirmation = _clamp(
        50.0
        + _signal_value(ma_signal) * 10.0
        + _signal_value(macd_signal) * 8.0
        + (5.0 if 45.0 <= rsi <= 65.0 else -5.0 if rsi >= 75.0 or rsi <= 30.0 else 0.0)
        + _scaled(volume_ratio - 1.0, 0.8) * 4.0
    )
    risk_efficiency = _clamp(
        72.0
        - min(vol_20d, 0.50) / 0.50 * 20.0
        - min(max_drawdown, 0.60) / 0.60 * 18.0
    )
    liquidity_profile = _clamp(40.0 + _scaled(median_turnover_60d / 1e8 - 1.0, 4.0) * 20.0)

    factor_scores = {
        "medium_term_momentum": medium_term_momentum,
        "benchmark_relative": benchmark_relative,
        "technical_confirmation": technical_confirmation,
        "risk_efficiency": risk_efficiency,
        "liquidity_profile": liquidity_profile,
    }
    normalized_weights = _normalize_weight_scheme(weight_scheme or STRATEGY_V1_REPLAY_WEIGHT_SCHEME)
    seed_score = round(
        sum(float(factor_scores[key]) * float(normalized_weights.get(key, 0.0)) for key in factor_scores)
        / sum(normalized_weights.values()),
        2,
    )
    factor_snapshot = {
        "factor_scores": {key: round(float(value), 2) for key, value in factor_scores.items()},
        "price_momentum": {
            "return_5d": _safe_float(metrics.get("return_5d")),
            "return_20d": return_20d,
            "return_60d": return_60d,
            "price_percentile_1y": price_percentile,
        },
        "benchmark_relative": {
            "benchmark_return_20d": benchmark_return_20d,
            "benchmark_return_60d": benchmark_return_60d,
            "relative_return_20d": relative_20d,
            "relative_return_60d": relative_60d,
        },
        "technical": {
            "ma_signal": ma_signal,
            "macd_signal": macd_signal,
            "rsi": rsi,
            "volume_ratio": volume_ratio,
        },
        "liquidity": {
            "avg_turnover_20d": _safe_float(metrics.get("avg_turnover_20d")),
            "median_turnover_60d": median_turnover_60d,
        },
        "risk": {
            "volatility_20d": vol_20d,
            "max_drawdown_1y": -max_drawdown,
        },
    }
    key_factors = [
        {
            "factor": "benchmark_relative",
            "label": "相对基准强弱",
            "score": round(benchmark_relative, 2),
            "direction": "supportive" if benchmark_relative >= 55 else "drag",
            "summary": f"20日相对中证800超额 `{relative_20d * 100:+.2f}%`，60日相对超额 `{relative_60d * 100:+.2f}%`。",
        },
        {
            "factor": "medium_term_momentum",
            "label": "中周期动量",
            "score": round(medium_term_momentum, 2),
            "direction": "supportive" if medium_term_momentum >= 55 else "drag",
            "summary": f"20日 `{return_20d * 100:+.2f}%`，60日 `{return_60d * 100:+.2f}%`，一年价格分位 `{price_percentile:.0%}`。",
        },
        {
            "factor": "technical_confirmation",
            "label": "技术确认",
            "score": round(technical_confirmation, 2),
            "direction": "supportive" if technical_confirmation >= 55 else "drag",
            "summary": f"MA `{ma_signal}`，MACD `{macd_signal}`，RSI `{rsi:.1f}`，量比 `{volume_ratio:.2f}`。",
        },
        {
            "factor": "risk_efficiency",
            "label": "风险效率",
            "score": round(risk_efficiency, 2),
            "direction": "supportive" if risk_efficiency >= 55 else "drag",
            "summary": f"20日波动 `{vol_20d * 100:.2f}%`，1年最大回撤 `{-max_drawdown * 100:.2f}%`。",
        },
        {
            "factor": "liquidity_profile",
            "label": "流动性画像",
            "score": round(liquidity_profile, 2),
            "direction": "supportive" if liquidity_profile >= 55 else "drag",
            "summary": f"60日中位成交额约 `{median_turnover_60d / 1e8:.2f}` 亿。",
        },
    ]
    key_factors.sort(key=lambda row: float(row.get("score", 0.0)), reverse=True)
    return {
        "seed_score": seed_score,
        "weight_scheme": normalized_weights,
        "factor_snapshot": factor_snapshot,
        "key_factors": key_factors,
    }


def _eligibility_checks(
    *,
    symbol: str,
    analysis: Mapping[str, Any],
    benchmark_history: pd.DataFrame,
    benchmark_fixture: Mapping[str, Any] | None = None,
    lag_visibility_fixture: Mapping[str, Any] | None = None,
) -> Tuple[List[str], List[str]]:
    reasons: List[str] = []
    codes: List[str] = []
    asset_type = str(analysis.get("asset_type", "")).strip()
    name = str(analysis.get("name", "")).upper()
    history = _safe_normalize_history(analysis.get("history"))

    def _add(code: str, message: str) -> None:
        if code not in codes:
            codes.append(code)
            reasons.append(message)

    if asset_type != "cn_stock":
        _add("unsupported_asset_type", "strategy v1 当前只接受 A 股普通股票，不对 ETF、基金、港美股直接给主预测。")
    if str(symbol).startswith(("43", "83", "87", "88")):
        _add("bj_market_excluded", "strategy v1 当前排除北交所股票，避免和首发 universe 合同冲突。")
    if "ST" in name:
        _add("st_stock_excluded", "策略 v1 当前排除 ST 标的。")
    if bool(analysis.get("history_fallback_mode")):
        _add("history_fallback_mode", "当前只能拿到降级历史/快照，不满足 strategy v1 的完整日线合同。")
    if history.empty:
        _add("missing_history", "没有拿到完整历史日线，无法建立 20 日超额收益预测账本。")
    elif len(history) < 250:
        _add("insufficient_history", f"历史长度只有 {len(history)} 个交易日，低于 v1 要求的 250 日。")

    median_turnover = _median_turnover_60d(history) if not history.empty else 0.0
    if median_turnover < 1e8:
        _add("low_liquidity", f"最近 60 个交易日中位成交额约 {median_turnover / 1e8:.2f} 亿，低于 1 亿门槛。")

    if not history.empty:
        recent = history.tail(20)
        zero_volume_ratio = float((pd.to_numeric(recent.get("volume"), errors="coerce").fillna(0.0) <= 0).mean())
        if zero_volume_ratio > 0.2:
            _add("halted_or_sparse_trading", "最近 20 日里零成交/近似停牌天数偏多，不满足正常流动性样本要求。")

    benchmark_normalized = _safe_normalize_history(benchmark_history)
    benchmark_fixture_block = dict(benchmark_fixture or {})
    if benchmark_normalized.empty:
        _add("benchmark_missing", "没有拿到中证800价格基准，主标签无法按合同定义。")
    else:
        blockers = {str(item) for item in list(benchmark_fixture_block.get("blockers") or [])}
        if "benchmark_overlap_insufficient" in blockers:
            _add(
                "benchmark_overlap_insufficient",
                (
                    "资产和中证800当前只有 "
                    f"{int(benchmark_fixture_block.get('overlap_rows') or 0)} 行可重叠历史，"
                    "低于 strategy v1 默认要求的 250 行 benchmark fixture。"
                ),
            )
        if "benchmark_as_of_misaligned" in blockers:
            _add(
                "benchmark_as_of_misaligned",
                (
                    f"资产 as_of `{benchmark_fixture_block.get('asset_as_of', '—')}` 和基准 as_of "
                    f"`{benchmark_fixture_block.get('benchmark_as_of', '—')}` 没有对齐，"
                    "当前不应按主合同写 benchmark-relative 预测。"
                ),
            )
    lag_visibility_block = dict(lag_visibility_fixture or {})
    lag_visibility_status = str(lag_visibility_block.get("status", ""))
    if lag_visibility_status == "missing":
        _add("lag_visibility_fixture_missing", "当前缺少可复核的 factor meta 样本，lag / visibility fixture 无法成立。")
    elif lag_visibility_status == "blocked":
        _add(
            "lag_visibility_fixture_blocked",
            (
                "当前没有足够的 point-in-time strategy candidate 因子通过 lag / visibility fixture，"
                "不应给 strategy v1 主预测。"
            ),
        )

    return codes, reasons


def _downgrade_flags(
    analysis: Mapping[str, Any],
    benchmark_history: pd.DataFrame,
    *,
    factor_contract: Mapping[str, Any] | None = None,
    benchmark_fixture: Mapping[str, Any] | None = None,
    lag_visibility_fixture: Mapping[str, Any] | None = None,
) -> List[str]:
    flags: List[str] = []
    provenance = dict(analysis.get("provenance") or {})
    catalyst = dict(dict(analysis.get("dimensions") or {}).get("catalyst") or {})
    coverage = dict(catalyst.get("coverage") or {})
    if bool(analysis.get("history_fallback_mode")):
        flags.append("history_fallback_mode")
    if coverage.get("degraded"):
        flags.append("catalyst_coverage_degraded")
    if str(provenance.get("intraday_as_of", "")) == "未启用":
        flags.append("intraday_not_used")
    if _safe_normalize_history(benchmark_history).empty:
        flags.append("benchmark_history_missing")
    if str(dict(benchmark_fixture or {}).get("status", "")) == "partial":
        flags.append("benchmark_fixture_partial")
    lag_visibility_status = str(dict(lag_visibility_fixture or {}).get("status", ""))
    if lag_visibility_status == "partial":
        flags.append("lag_visibility_fixture_partial")
    elif lag_visibility_status == "blocked":
        flags.append("lag_visibility_fixture_blocked")
    elif lag_visibility_status == "missing":
        flags.append("lag_visibility_fixture_missing")
    blockers = list(dict(factor_contract or {}).get("point_in_time_blockers") or [])
    if blockers:
        flags.append("factor_contract_pti_blockers")
    return flags


def _key_factors(analysis: Mapping[str, Any], score_map: Mapping[str, float]) -> List[Dict[str, Any]]:
    dimensions = dict(analysis.get("dimensions") or {})
    ranked = sorted(score_map.items(), key=lambda item: item[1], reverse=True)
    weakest = [item for item in sorted(score_map.items(), key=lambda item: item[1]) if item[1] < 45][:2]
    selected: List[Tuple[str, float]] = []
    selected_keys: set[str] = set()
    for key, score in ranked[:3] + weakest:
        if key in selected_keys:
            continue
        selected.append((key, score))
        selected_keys.add(key)
    rows: List[Dict[str, Any]] = []
    for key, score in selected:
        payload = dict(dimensions.get(key) or {})
        rows.append(
            {
                "factor": key,
                "label": _DIMENSION_LABELS.get(key, key),
                "score": round(float(score), 2),
                "direction": "supportive" if score >= 55 else "drag",
                "summary": str(payload.get("summary", "")).strip(),
            }
        )
    return rows


def _factor_snapshot(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    metrics = dict(analysis.get("metrics") or {})
    technical = dict(analysis.get("technical_raw") or {})
    return {
        "price_momentum": {
            "return_5d": _safe_float(metrics.get("return_5d")),
            "return_20d": _safe_float(metrics.get("return_20d")),
            "return_60d": _safe_float(metrics.get("return_60d")),
            "price_percentile_1y": _safe_float(metrics.get("price_percentile_1y"), 0.5),
        },
        "liquidity": {
            "avg_turnover_20d": _safe_float(metrics.get("avg_turnover_20d")),
            "median_turnover_60d": _median_turnover_60d(analysis.get("history")),
        },
        "risk": {
            "volatility_20d": _safe_float(metrics.get("volatility_20d")),
            "max_drawdown_1y": _safe_float(metrics.get("max_drawdown_1y")),
        },
        "technical": {
            "ma_signal": str(dict(technical.get("ma_system") or {}).get("signal", "")).lower(),
            "macd_signal": str(dict(technical.get("macd") or {}).get("signal", "")).lower(),
            "rsi": _safe_float(dict(technical.get("rsi") or {}).get("RSI")),
            "volume_ratio": _safe_float(dict(technical.get("volume") or {}).get("vol_ratio"), 1.0),
        },
    }


def _evidence_sources(analysis: Mapping[str, Any], benchmark_history: pd.DataFrame) -> Dict[str, Any]:
    provenance = dict(analysis.get("provenance") or {})
    return {
        "market_data_as_of": str(provenance.get("market_data_as_of", "—")),
        "market_data_source": str(provenance.get("market_data_source", "")),
        "benchmark_as_of": _safe_history_as_of(benchmark_history),
        "benchmark_source": f"{STRATEGY_V1_BENCHMARK_SYMBOL} 日线历史",
        "catalyst_evidence_as_of": str(provenance.get("catalyst_evidence_as_of", "—")),
        "catalyst_sources": list(provenance.get("catalyst_sources") or []),
        "point_in_time_note": str(provenance.get("point_in_time_note", "")),
        "notes": list(provenance.get("notes") or []),
    }


def _regime_snapshot(analysis: Mapping[str, Any]) -> Dict[str, Any]:
    regime = dict(analysis.get("regime") or {})
    day_theme = dict(analysis.get("day_theme") or {})
    return {
        "macro_regime": str(regime.get("label", "") or regime.get("state", "")),
        "macro_bias": str(regime.get("summary", "") or regime.get("description", "")),
        "day_theme": str(day_theme.get("label", "") or day_theme.get("theme", "")),
    }


def _visibility_class(analysis: Mapping[str, Any]) -> str:
    if bool(analysis.get("history_fallback_mode")):
        return "degraded_snapshot_only_v1"
    return "post_close_t_plus_1_v1"


def build_strategy_prediction_from_analysis(
    analysis: Mapping[str, Any],
    *,
    benchmark_history: pd.DataFrame,
    note: str = "",
) -> Dict[str, Any]:
    symbol = str(analysis.get("symbol", "")).strip()
    as_of = str(dict(analysis.get("provenance") or {}).get("market_data_as_of", "")) or history_as_of(analysis.get("history"))
    benchmark_fixture = _benchmark_fixture(
        analysis.get("history"),
        benchmark_history,
        as_of=as_of,
        horizon_days=20,
    )
    score_map = _dimension_score_map(dict(analysis.get("dimensions") or {}))
    seed_score = _seed_rank_score(score_map)
    confidence = _confidence_payload(seed_score)
    prediction_value = _prediction_value(seed_score)
    factor_contract = summarize_factor_contracts_from_analysis(analysis)
    lag_visibility_fixture = _lag_visibility_fixture(factor_contract, mode="prediction")
    no_prediction_codes, no_prediction_reasons = _eligibility_checks(
        symbol=symbol,
        analysis=analysis,
        benchmark_history=benchmark_history,
        benchmark_fixture=benchmark_fixture,
        lag_visibility_fixture=lag_visibility_fixture,
    )
    created_at = datetime.now(UTC).isoformat(timespec="seconds")
    payload = {
        "prediction_id": f"stratv1_{symbol}_{created_at.replace(':', '').replace('+00:00', 'z')}",
        "status": "no_prediction" if no_prediction_codes else "predicted",
        "created_at": created_at,
        "as_of": as_of,
        "effective_from": _candidate_effective_from(as_of),
        "visibility_class": _visibility_class(analysis),
        "symbol": symbol,
        "name": str(analysis.get("name", symbol)),
        "asset_type": str(analysis.get("asset_type", "")),
        "universe": STRATEGY_V1_UNIVERSE,
        "horizon": {"days": 20, "label": "20个交易日"},
        "prediction_target": STRATEGY_V1_TARGET,
        "prediction_value": prediction_value,
        "seed_score": seed_score,
        "confidence": confidence["score"],
        "confidence_bucket": confidence["bucket"],
        "confidence_label": confidence["label"],
        "confidence_type": "rank_confidence_v1",
        "key_factors": _key_factors(analysis, score_map),
        "factor_snapshot": _factor_snapshot(analysis),
        "factor_contract": factor_contract,
        "factor_version": STRATEGY_V1_FACTOR_VERSION,
        "weight_scheme": dict(STRATEGY_V1_WEIGHT_SCHEME),
        "benchmark": {
            "symbol": STRATEGY_V1_BENCHMARK_SYMBOL,
            "name": STRATEGY_V1_BENCHMARK_NAME,
            "as_of": _safe_history_as_of(benchmark_history),
        },
        "benchmark_fixture": benchmark_fixture,
        "lag_visibility_fixture": lag_visibility_fixture,
        "regime": _regime_snapshot(analysis),
        "evidence_sources": _evidence_sources(analysis, benchmark_history),
        "downgrade_flags": _downgrade_flags(
            analysis,
            benchmark_history,
            factor_contract=factor_contract,
            benchmark_fixture=benchmark_fixture,
            lag_visibility_fixture=lag_visibility_fixture,
        ),
        "no_prediction_reason_codes": no_prediction_codes,
        "no_prediction_reasons": no_prediction_reasons,
        "cohort_contract": {
            "snapshot_frequency": "weekly_close",
            "cohort_frequency_days": 5,
            "holding_period_days": 20,
            "overlap_policy": "no_new_primary_sample_before_previous_20d_window_finishes",
        },
        "notes": [item for item in [str(note).strip()] if item],
    }
    rows_with_overlap, _ = _attach_overlap_fixtures([payload], lambda _symbol, _asset_type: _safe_normalize_history(analysis.get("history")))
    return rows_with_overlap[0]


def generate_strategy_prediction(symbol: str, config: Mapping[str, Any], *, note: str = "") -> Dict[str, Any]:
    asset_type = detect_asset_type(symbol, config)
    context = build_market_context(dict(config), relevant_asset_types=["cn_stock", "cn_index", "cn_etf", "futures"])
    analysis = analyze_opportunity(symbol, asset_type, config, context=context, today_mode=False)
    try:
        benchmark_history = fetch_asset_history(STRATEGY_V1_BENCHMARK_SYMBOL, "cn_index", dict(config))
    except Exception:
        benchmark_history = pd.DataFrame()
    return build_strategy_prediction_from_analysis(analysis, benchmark_history=benchmark_history, note=note)


def _build_replay_prediction(
    *,
    symbol: str,
    name: str,
    asset_type: str,
    asset_history: pd.DataFrame,
    benchmark_history: pd.DataFrame,
    note: str = "",
    weight_scheme: Mapping[str, Any] | None = None,
    factor_version: str = STRATEGY_V1_REPLAY_FACTOR_VERSION,
    prediction_mode: str = "historical_replay_v1",
    experiment_variant: str = "",
    asset_reentry_gap_days: int = STRATEGY_V1_ASSET_GAP_DAYS,
) -> Dict[str, Any]:
    as_of = _safe_history_as_of(asset_history)
    benchmark_fixture = _benchmark_fixture(
        asset_history,
        benchmark_history,
        as_of=as_of,
        horizon_days=20,
    )
    lag_visibility_fixture = _lag_visibility_fixture({}, mode="replay_price_only_v1")
    base_payload = {
        "symbol": symbol,
        "name": name,
        "asset_type": asset_type,
        "history": asset_history,
        "history_fallback_mode": False,
    }
    no_prediction_codes, no_prediction_reasons = _eligibility_checks(
        symbol=symbol,
        analysis=base_payload,
        benchmark_history=benchmark_history,
        benchmark_fixture=benchmark_fixture,
        lag_visibility_fixture=lag_visibility_fixture,
    )
    scorecard = {
        "weight_scheme": dict(weight_scheme or STRATEGY_V1_REPLAY_WEIGHT_SCHEME),
        "seed_score": 50.0,
        "factor_snapshot": {
            "factor_scores": {},
            "price_momentum": {},
            "benchmark_relative": {},
            "technical": {},
            "liquidity": {},
            "risk": {},
        },
        "factor_contract": {
            "registered_factor_rows": 0,
            "families": {},
            "states": {},
            "visibility_classes": {},
            "proxy_levels": {},
            "fixture_readiness": {
                "total_factors": 0,
                "lag_ready_count": 0,
                "lag_blocked_count": 0,
                "visibility_ready_count": 0,
                "visibility_blocked_count": 0,
                "point_in_time_ready_count": 0,
                "point_in_time_blocked_count": 0,
                "strategy_candidate_total": 0,
                "strategy_candidate_ready_count": 0,
                "degraded_count": 0,
                "max_lag_days": 0,
            },
            "strategy_candidate_factor_ids": [],
            "point_in_time_blockers": [],
            "lag_visibility_blockers": [],
            "degraded_factor_ids": [],
            "sample_rows": [],
        },
        "key_factors": [],
    }
    if not no_prediction_codes:
        scorecard = _replay_factor_engine(asset_history, benchmark_history, weight_scheme=weight_scheme)
    seed_score = float(scorecard.get("seed_score", 50.0))
    confidence = _confidence_payload(seed_score)
    prediction_value = _prediction_value(seed_score)
    notes = [
        "这是单标的时间序列 replay，当前更适合验证方向和分桶校准，不等于全市场截面 rank 的完整替代。"
    ]
    if note:
        notes.insert(0, str(note).strip())
    return {
        "prediction_id": f"stratv1_replay_{symbol}_{as_of}",
        "status": "no_prediction" if no_prediction_codes else "predicted",
        "created_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "as_of": as_of,
        "effective_from": _candidate_effective_from(as_of),
        "visibility_class": "historical_replay_post_close_v1",
        "prediction_mode": prediction_mode,
        "experiment_variant": experiment_variant,
        "symbol": symbol,
        "name": name,
        "asset_type": asset_type,
        "universe": STRATEGY_V1_UNIVERSE,
        "horizon": {"days": 20, "label": "20个交易日"},
        "prediction_target": STRATEGY_V1_TARGET,
        "prediction_value": prediction_value,
        "seed_score": seed_score,
        "confidence": confidence["score"],
        "confidence_bucket": confidence["bucket"],
        "confidence_label": confidence["label"],
        "confidence_type": "rank_confidence_v1",
        "key_factors": list(scorecard.get("key_factors") or []),
        "factor_snapshot": dict(scorecard.get("factor_snapshot") or {}),
        "factor_contract": dict(scorecard.get("factor_contract") or {}),
        "factor_version": factor_version,
        "weight_scheme": dict(scorecard.get("weight_scheme") or {}),
        "benchmark": {
            "symbol": STRATEGY_V1_BENCHMARK_SYMBOL,
            "name": STRATEGY_V1_BENCHMARK_NAME,
            "as_of": _safe_history_as_of(benchmark_history),
        },
        "benchmark_fixture": benchmark_fixture,
        "lag_visibility_fixture": lag_visibility_fixture,
        "regime": {
            "macro_regime": "replay_price_only_v1",
            "macro_bias": "历史回放当前只使用当时可见的量价、流动性和基准相对强弱，不纳入新闻/政策/财报修订。",
            "day_theme": "",
        },
        "evidence_sources": {
            "market_data_as_of": as_of,
            "market_data_source": "历史日线 replay 截断样本",
            "benchmark_as_of": _safe_history_as_of(benchmark_history),
            "benchmark_source": f"{STRATEGY_V1_BENCHMARK_SYMBOL} 历史日线 replay 截断样本",
            "catalyst_evidence_as_of": "未纳入 replay v1",
            "catalyst_sources": [],
            "point_in_time_note": "历史 replay v1 只使用该时点及之前的日线量价、流动性和基准相对收益，不把后验新闻/财报/政策回填进样本。",
            "notes": [
                "当前 replay 仍是单标的时间序列口径，验证的是超额收益方向和分桶校准，不是完整截面 rank 质量。"
            ],
        },
        "downgrade_flags": ["replay_price_only_v1", "single_symbol_time_series_validation_only"],
        "no_prediction_reason_codes": no_prediction_codes,
        "no_prediction_reasons": no_prediction_reasons,
        "cohort_contract": {
            "snapshot_frequency": "historical_replay",
            "cohort_frequency_days": 5,
            "holding_period_days": 20,
            "asset_reentry_gap_days": max(int(asset_reentry_gap_days), 1),
            "overlap_policy": "single_symbol replay keeps primary samples non-overlapping by default",
        },
        "notes": notes,
    }


def _replay_sample_indices(
    asset_history: pd.DataFrame,
    *,
    start: str = "",
    end: str = "",
    asset_gap_days: int = STRATEGY_V1_ASSET_GAP_DAYS,
    max_samples: int = 12,
) -> Tuple[pd.Timestamp, pd.Timestamp, List[int]]:
    if asset_history.empty:
        raise ValueError("asset history is empty")
    start_stamp = pd.Timestamp(start) if start else pd.Timestamp(asset_history["date"].iloc[min(len(asset_history) - 1, 249)])
    end_stamp = pd.Timestamp(end) if end else pd.Timestamp(asset_history["date"].iloc[-1])
    indices: List[int] = []
    last_used_index = -10_000
    eligible_indices = [
        index
        for index, stamp in enumerate(asset_history["date"])
        if index >= 249 and pd.Timestamp(stamp) >= start_stamp and pd.Timestamp(stamp) <= end_stamp
    ]
    for index in eligible_indices:
        if index - last_used_index < max(int(asset_gap_days), 1):
            continue
        indices.append(index)
        last_used_index = index
        if max_samples and len(indices) >= max(int(max_samples), 1):
            break
    return start_stamp, end_stamp, indices


def generate_strategy_replay_predictions(
    symbol: str,
    config: Mapping[str, Any],
    *,
    start: str = "",
    end: str = "",
    note: str = "",
    asset_gap_days: int = STRATEGY_V1_ASSET_GAP_DAYS,
    max_samples: int = 12,
    batch_context: Mapping[str, Any] | None = None,
    cohort_recipe: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    asset_type = detect_asset_type(symbol, config)
    asset_history = _safe_normalize_history(fetch_asset_history(symbol, asset_type, dict(config)))
    benchmark_history = _safe_normalize_history(fetch_asset_history(STRATEGY_V1_BENCHMARK_SYMBOL, "cn_index", dict(config)))
    if asset_history.empty:
        raise ValueError(f"无法生成历史 replay，缺少 {symbol} 的完整日线。")
    start_stamp, end_stamp, sample_indices = _replay_sample_indices(
        asset_history,
        start=start,
        end=end,
        asset_gap_days=asset_gap_days,
        max_samples=max_samples,
    )
    rows: List[Dict[str, Any]] = []
    for index in sample_indices:
        asset_slice = asset_history.iloc[: index + 1].copy()
        as_of = pd.Timestamp(asset_slice["date"].iloc[-1])
        benchmark_slice = benchmark_history[benchmark_history["date"] <= as_of].copy()
        prediction = _build_replay_prediction(
            symbol=symbol,
            name=str(symbol),
            asset_type=asset_type,
            asset_history=asset_slice,
            benchmark_history=benchmark_slice,
            note=note,
            asset_reentry_gap_days=asset_gap_days,
        )
        rows.append(prediction)
    rows, overlap_fixture_summary = _attach_overlap_fixtures(
        rows,
        lambda _symbol, _asset_type: asset_history,
    )
    notes = [
        "当前 replay 默认按单标的 non-overlap 主样本生成。",
        "这一步先建立历史样本，不代表已经完成全市场截面排序验证。",
    ]
    if int(overlap_fixture_summary.get("violation_count") or 0) > 0:
        notes.append("当前 replay 样本存在 primary window 重叠，这批结果更适合做敏感性观察，不应直接当成 non-overlap 主样本验证。")
    return {
        "symbol": symbol,
        "asset_type": asset_type,
        "scope": "single_symbol_historical_replay_v1",
        "symbols": [symbol],
        "symbol_count": 1,
        "start": str(start_stamp.date()),
        "end": str(end_stamp.date()),
        "asset_gap_days": max(int(asset_gap_days), 1),
        "batch_context": dict(batch_context or {}),
        "cohort_recipe": dict(cohort_recipe or {}),
        "rows": rows,
        "symbol_rows": [
            {
                "symbol": symbol,
                "status": "ready",
                "sample_count": len(rows),
                "predicted_count": sum(1 for row in rows if str(row.get("status", "")) == "predicted"),
                "no_prediction_count": sum(1 for row in rows if str(row.get("status", "")) == "no_prediction"),
                "first_as_of": str(rows[0].get("as_of", "—")) if rows else "—",
                "last_as_of": str(rows[-1].get("as_of", "—")) if rows else "—",
                "error": "",
            }
        ],
        "cross_sectional_supply_summary": {},
        "benchmark_fixture_summary": _aggregate_benchmark_fixtures(rows),
        "lag_visibility_fixture_summary": _aggregate_lag_visibility_fixtures(rows),
        "overlap_fixture_summary": overlap_fixture_summary,
        "notes": notes,
    }


def _cross_sectional_supply_summary(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    grouped: Dict[str, Dict[str, Any]] = {}
    unique_symbols: set[str] = set()
    for row in rows:
        as_of = str(row.get("as_of", "")).strip()
        symbol = str(row.get("symbol", "")).strip()
        if not as_of or not symbol:
            continue
        unique_symbols.add(symbol)
        bucket = grouped.setdefault(
            as_of,
            {
                "symbols": set(),
                "predicted_count": 0,
                "no_prediction_count": 0,
            },
        )
        bucket["symbols"].add(symbol)
        if str(row.get("status", "")) == "predicted":
            bucket["predicted_count"] += 1
        elif str(row.get("status", "")) == "no_prediction":
            bucket["no_prediction_count"] += 1

    cohort_rows: List[Dict[str, Any]] = []
    for as_of in sorted(grouped):
        bucket = grouped[as_of]
        cohort_rows.append(
            {
                "as_of": as_of,
                "symbol_count": len(set(bucket.get("symbols") or set())),
                "predicted_count": int(bucket.get("predicted_count", 0)),
                "no_prediction_count": int(bucket.get("no_prediction_count", 0)),
            }
        )

    cohort_count = len(cohort_rows)
    counts = [int(row.get("symbol_count", 0)) for row in cohort_rows]
    cohorts_ge_2 = sum(1 for count in counts if count >= 2)
    cohorts_ge_3 = sum(1 for count in counts if count >= 3)
    max_symbols_per_as_of = max(counts) if counts else 0
    min_symbols_per_as_of = min(counts) if counts else 0
    if not cohort_rows:
        summary = "当前还没有可复核的同日 replay cohort。"
    elif cohorts_ge_3 > 0:
        summary = f"当前已有 `{cohorts_ge_3}` 个日期至少覆盖 3 只标的，可以开始积累 cross-sectional validate 样本。"
    elif cohorts_ge_2 > 0:
        summary = "当前已有部分同日 cohort，但离最小 3-symbol cross-sectional cohort 还差一步。"
    else:
        summary = "当前 replay 仍主要是单标的日期，尚未形成可用的同日 cross-sectional cohort。"
    return {
        "cohort_count": cohort_count,
        "unique_symbol_count": len(unique_symbols),
        "cohorts_ge_2": cohorts_ge_2,
        "cohorts_ge_3": cohorts_ge_3,
        "min_symbols_per_as_of": min_symbols_per_as_of,
        "max_symbols_per_as_of": max_symbols_per_as_of,
        "cohort_rows": cohort_rows[:12],
        "summary": summary,
    }


def _normalize_strategy_symbols(symbols: Sequence[str]) -> List[str]:
    normalized_symbols: List[str] = []
    seen_symbols: set[str] = set()
    for item in symbols:
        for token in str(item or "").split(","):
            symbol = token.strip()
            if symbol and symbol not in seen_symbols:
                seen_symbols.add(symbol)
                normalized_symbols.append(symbol)
    return normalized_symbols


def generate_strategy_multi_symbol_replay_predictions(
    symbols: Sequence[str],
    config: Mapping[str, Any],
    *,
    start: str = "",
    end: str = "",
    note: str = "",
    asset_gap_days: int = STRATEGY_V1_ASSET_GAP_DAYS,
    max_samples: int = 12,
    batch_context: Mapping[str, Any] | None = None,
    cohort_recipe: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_symbols = _normalize_strategy_symbols(symbols)
    if not normalized_symbols:
        raise ValueError("至少需要一个 symbol 才能生成 strategy replay。")

    all_rows: List[Dict[str, Any]] = []
    symbol_rows: List[Dict[str, Any]] = []
    successful_payloads: List[Dict[str, Any]] = []

    for symbol in normalized_symbols:
        try:
            payload = generate_strategy_replay_predictions(
                symbol,
                config,
                start=start,
                end=end,
                note=note,
                asset_gap_days=asset_gap_days,
                max_samples=max_samples,
                batch_context=batch_context,
                cohort_recipe=cohort_recipe,
            )
        except Exception as exc:
            symbol_rows.append(
                {
                    "symbol": symbol,
                    "status": "failed",
                    "sample_count": 0,
                    "predicted_count": 0,
                    "no_prediction_count": 0,
                    "first_as_of": "—",
                    "last_as_of": "—",
                    "error": str(exc),
                }
            )
            continue

        successful_payloads.append(payload)
        rows = [dict(row) for row in list(payload.get("rows") or [])]
        all_rows.extend(rows)
        symbol_rows.append(
            {
                "symbol": symbol,
                "status": "ready",
                "sample_count": len(rows),
                "predicted_count": sum(1 for row in rows if str(row.get("status", "")) == "predicted"),
                "no_prediction_count": sum(1 for row in rows if str(row.get("status", "")) == "no_prediction"),
                "first_as_of": str(rows[0].get("as_of", "—")) if rows else "—",
                "last_as_of": str(rows[-1].get("as_of", "—")) if rows else "—",
                "error": "",
            }
        )

    all_rows.sort(
        key=lambda row: (
            str(row.get("as_of", "")),
            str(row.get("symbol", "")),
            str(row.get("created_at", "")),
            str(row.get("prediction_id", "")),
        )
    )
    supply_summary = _cross_sectional_supply_summary(all_rows)
    notes = [
        "当前 replay 已扩到多标的样本供给；cross-sectional validate 是否生效取决于同日 cohort 覆盖。",
        "这一步的目标先是把样本供给补齐，不代表已经完成多标的 promotion / experiment。",
    ]
    failed_count = sum(1 for row in symbol_rows if str(row.get("status", "")) == "failed")
    if failed_count > 0:
        notes.append(f"有 `{failed_count}` 只标的没能生成 replay 样本，需要先解决历史日线或 universe 合同问题。")
    if int(supply_summary.get("cohorts_ge_3", 0)) <= 0:
        notes.append("当前还没有形成满足 3-symbol 门槛的同日 cohort，cross-sectional validate 仍会保持 blocked。")

    start_value = start
    end_value = end
    if successful_payloads:
        if not start_value:
            start_value = min(str(payload.get("start", "")) for payload in successful_payloads if str(payload.get("start", "")).strip())
        if not end_value:
            end_value = max(str(payload.get("end", "")) for payload in successful_payloads if str(payload.get("end", "")).strip())

    return {
        "symbol": ",".join(normalized_symbols),
        "symbols": normalized_symbols,
        "symbol_count": len(normalized_symbols),
        "scope": "multi_symbol_historical_replay_supply_v1",
        "start": start_value,
        "end": end_value,
        "asset_gap_days": max(int(asset_gap_days), 1),
        "batch_context": dict(batch_context or {}),
        "cohort_recipe": dict(cohort_recipe or {}),
        "rows": all_rows,
        "symbol_rows": symbol_rows,
        "cross_sectional_supply_summary": supply_summary,
        "benchmark_fixture_summary": _aggregate_benchmark_fixtures(all_rows),
        "lag_visibility_fixture_summary": _aggregate_lag_visibility_fixtures(all_rows),
        "overlap_fixture_summary": _aggregate_overlap_fixtures(all_rows),
        "notes": notes,
    }


def _locate_as_of_index(history: pd.DataFrame, as_of: Any) -> int:
    normalized = _safe_normalize_history(history)
    if normalized.empty:
        return -1
    stamp = pd.Timestamp(as_of)
    matches = normalized.index[normalized["date"] <= stamp]
    if len(matches) <= 0:
        return -1
    return int(matches[-1])


def _validation_snapshot(
    row: Mapping[str, Any],
    *,
    asset_history: pd.DataFrame,
    benchmark_history: pd.DataFrame,
) -> Dict[str, Any]:
    if str(row.get("status", "")) != "predicted":
        return {
            "validation_status": "skipped_no_prediction",
            "evaluated": False,
            "reason": "sample was intentionally rejected by strategy v1 gating",
        }
    asset_index = _locate_as_of_index(asset_history, row.get("as_of"))
    benchmark_index = _locate_as_of_index(benchmark_history, row.get("as_of"))
    horizon_days = int(dict(row.get("horizon") or {}).get("days", 20) or 20)
    if asset_index < 0 or benchmark_index < 0:
        return {
            "validation_status": "not_evaluable_due_to_data_quality",
            "evaluated": False,
            "reason": "missing as_of row in asset or benchmark history",
        }
    asset_frame = _safe_normalize_history(asset_history)
    benchmark_frame = _safe_normalize_history(benchmark_history)
    if asset_index + horizon_days >= len(asset_frame) or benchmark_index + horizon_days >= len(benchmark_frame):
        return {
            "validation_status": "pending_future_window",
            "evaluated": False,
            "reason": "future 20-trading-day window is not fully available yet",
        }
    start_price = float(asset_frame["close"].iloc[asset_index])
    end_price = float(asset_frame["close"].iloc[asset_index + horizon_days])
    benchmark_start = float(benchmark_frame["close"].iloc[benchmark_index])
    benchmark_end = float(benchmark_frame["close"].iloc[benchmark_index + horizon_days])
    asset_return = end_price / start_price - 1 if start_price else 0.0
    benchmark_return = benchmark_end / benchmark_start - 1 if benchmark_start else 0.0
    excess_return = asset_return - benchmark_return
    future_window = asset_frame["close"].iloc[asset_index : asset_index + horizon_days + 1]
    max_drawdown = float((future_window / start_price - 1).min()) if start_price else 0.0
    direction = str(dict(row.get("prediction_value") or {}).get("expected_excess_direction", "neutral"))
    if direction == "positive":
        hit = excess_return > 0
        directional_excess = excess_return
        cost_adjusted = directional_excess - STRATEGY_V1_DIRECTIONAL_COST_BPS
    elif direction == "negative":
        hit = excess_return < 0
        directional_excess = -excess_return
        cost_adjusted = directional_excess - STRATEGY_V1_DIRECTIONAL_COST_BPS
    else:
        hit = abs(excess_return) <= STRATEGY_V1_NEUTRAL_BAND
        directional_excess = -abs(excess_return)
        cost_adjusted = directional_excess
    return {
        "validation_status": "validated",
        "evaluated": True,
        "window_start": str(pd.Timestamp(asset_frame["date"].iloc[asset_index]).date()),
        "window_end": str(pd.Timestamp(asset_frame["date"].iloc[asset_index + horizon_days]).date()),
        "realized_return": float(asset_return),
        "benchmark_return": float(benchmark_return),
        "excess_return": float(excess_return),
        "directional_excess_return": float(directional_excess),
        "cost_bps": int(STRATEGY_V1_DIRECTIONAL_COST_BPS * 10_000),
        "cost_adjusted_directional_return": float(cost_adjusted),
        "max_drawdown": float(max_drawdown),
        "hit": bool(hit),
        "neutral_band": float(STRATEGY_V1_NEUTRAL_BAND),
        "direction_checked": direction,
    }


def validate_strategy_rows(
    rows: Sequence[Mapping[str, Any]],
    config: Mapping[str, Any],
) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    cache: Dict[str, pd.DataFrame] = {}

    def _history(symbol: str, asset_type: str) -> pd.DataFrame:
        key = f"{asset_type}:{symbol}"
        if key not in cache:
            cache[key] = _safe_normalize_history(fetch_asset_history(symbol, asset_type, dict(config)))
        return cache[key]

    benchmark_history = _history(STRATEGY_V1_BENCHMARK_SYMBOL, "cn_index")
    updated_rows: List[Dict[str, Any]] = []
    evaluable: List[Dict[str, Any]] = []
    pending = 0
    skipped = 0

    for row in rows:
        cloned = dict(row)
        asset_history = _history(str(row.get("symbol", "")), str(row.get("asset_type", "")))
        lag_visibility_fixture = dict(cloned.get("lag_visibility_fixture") or {})
        if not lag_visibility_fixture:
            prediction_mode = str(cloned.get("prediction_mode", "")).strip()
            if prediction_mode in {"historical_replay_v1", "historical_experiment_v1"}:
                lag_visibility_fixture = _lag_visibility_fixture({}, mode="replay_price_only_v1")
            else:
                lag_visibility_fixture = _lag_visibility_fixture(cloned.get("factor_contract") or {}, mode="prediction")
        cloned["lag_visibility_fixture"] = lag_visibility_fixture
        as_of = row.get("as_of")
        asset_slice = asset_history[asset_history["date"] <= pd.Timestamp(as_of)].copy() if as_of else asset_history.copy()
        benchmark_slice = benchmark_history[benchmark_history["date"] <= pd.Timestamp(as_of)].copy() if as_of else benchmark_history.copy()
        cloned["benchmark_fixture"] = _benchmark_fixture(
            asset_slice,
            benchmark_slice,
            as_of=as_of,
            horizon_days=int(dict(cloned.get("horizon") or {}).get("days", 20) or 20),
        )
        validation = _validation_snapshot(cloned, asset_history=asset_history, benchmark_history=benchmark_history)
        cloned["validation"] = validation
        updated_rows.append(cloned)
        if validation.get("validation_status") == "validated":
            evaluable.append(cloned)
        elif validation.get("validation_status") == "pending_future_window":
            pending += 1
        elif validation.get("validation_status") == "skipped_no_prediction":
            skipped += 1

    updated_rows, overlap_fixture_summary = _attach_overlap_fixtures(updated_rows, _history)
    out_of_sample_validation = _out_of_sample_validation(updated_rows, overlap_fixture_summary=overlap_fixture_summary)
    chronological_cohort_validation = _chronological_cohort_validation(updated_rows)
    cross_sectional_validation = _cross_sectional_validation(updated_rows)
    rollback_gate = _rollback_gate(updated_rows, overlap_fixture_summary=overlap_fixture_summary, current_label="current_batch")

    hit_count = sum(1 for row in evaluable if dict(row.get("validation") or {}).get("hit"))
    avg_excess = sum(float(dict(row.get("validation") or {}).get("excess_return", 0.0)) for row in evaluable) / len(evaluable) if evaluable else 0.0
    avg_net = sum(float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return", 0.0)) for row in evaluable) / len(evaluable) if evaluable else 0.0
    avg_drawdown = sum(float(dict(row.get("validation") or {}).get("max_drawdown", 0.0)) for row in evaluable) / len(evaluable) if evaluable else 0.0
    bucket_rows: List[Dict[str, Any]] = []
    for bucket in ("高", "中", "低"):
        bucket_samples = [row for row in evaluable if str(row.get("confidence_label", "")) == bucket]
        if not bucket_samples:
            continue
        bucket_rows.append(
            {
                "bucket": bucket,
                "count": len(bucket_samples),
                "hit_rate": sum(1 for row in bucket_samples if dict(row.get("validation") or {}).get("hit")) / len(bucket_samples),
                "avg_excess_return": sum(float(dict(row.get("validation") or {}).get("excess_return", 0.0)) for row in bucket_samples) / len(bucket_samples),
                "avg_net_directional_return": sum(
                    float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return", 0.0))
                    for row in bucket_samples
                ) / len(bucket_samples),
            }
        )
    recent_rows = [
        {
            "as_of": str(row.get("as_of", "")),
            "symbol": str(row.get("symbol", "")),
            "direction": str(dict(row.get("prediction_value") or {}).get("expected_excess_direction", "")),
            "confidence_label": str(row.get("confidence_label", "")),
            "excess_return": float(dict(row.get("validation") or {}).get("excess_return", 0.0)),
            "net_directional_return": float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return", 0.0)),
            "hit": bool(dict(row.get("validation") or {}).get("hit", False)),
            "validation_status": str(dict(row.get("validation") or {}).get("validation_status", "")),
        }
        for row in updated_rows[:10]
    ]
    summary = {
        "total_rows": len(updated_rows),
        "predicted_rows": sum(1 for row in updated_rows if str(row.get("status", "")) == "predicted"),
        "no_prediction_rows": sum(1 for row in updated_rows if str(row.get("status", "")) == "no_prediction"),
        "validated_rows": len(evaluable),
        "pending_rows": pending,
        "skipped_rows": skipped,
        "hit_rate": hit_count / len(evaluable) if evaluable else 0.0,
        "avg_excess_return": avg_excess,
        "avg_cost_adjusted_directional_return": avg_net,
        "avg_max_drawdown": avg_drawdown,
        "benchmark_fixture_summary": _aggregate_benchmark_fixtures(updated_rows),
        "lag_visibility_fixture_summary": _aggregate_lag_visibility_fixtures(updated_rows),
        "overlap_fixture_summary": overlap_fixture_summary,
        "out_of_sample_validation": out_of_sample_validation,
        "chronological_cohort_validation": chronological_cohort_validation,
        "cross_sectional_validation": cross_sectional_validation,
        "rollback_gate": rollback_gate,
        "bucket_rows": bucket_rows,
        "recent_rows": recent_rows,
        "notes": [
            "当前 validate 先做单标的时间序列口径，核心看超额收益方向、成本后方向收益和置信度分桶校准。",
            "当前已补上 cross-sectional validate v1，但只有同日多标的 cohort 足够时才会生效，离全 universe rank 验证仍有距离。",
            *(
                ["当前样本里仍有 primary window overlap，验证统计不应直接拿来做 promotion gate。"]
                if int(overlap_fixture_summary.get("violation_count") or 0) > 0
                else []
            ),
            *(
                ["当前 out-of-sample validate 已进入 watchlist，最近 holdout 需要单独跟踪，不能只看整体均值。"]
                if str(out_of_sample_validation.get("status", "")) == "watchlist"
                else []
            ),
            *(
                ["当前 cross-sectional validate 还没拿到足够的同日多标的 cohort，不能把这批结果包装成横截面 rank 证明。"]
                if str(cross_sectional_validation.get("status", "")) == "blocked"
                else []
            ),
            *(
                ["当前 cross-sectional validate 已进入 watchlist，高 score 组相对低 score 组的优势还不稳定。"]
                if str(cross_sectional_validation.get("status", "")) == "watchlist"
                else []
            ),
            *(
                ["当前 rollback gate 已进入 watchlist，下一轮要重点看结构性 miss 是否持续。"]
                if str(rollback_gate.get("status", "")) == "watchlist"
                else []
            ),
            *(
                ["当前 rollback gate 已进入 rollback 候选讨论，不应继续把这批结果当成稳定 baseline 证明。"]
                if str(rollback_gate.get("status", "")) == "rollback_candidate"
                else []
            ),
        ],
    }
    return updated_rows, summary


def _factor_direction_counts(row: Mapping[str, Any]) -> Tuple[int, int]:
    supportive = 0
    drag = 0
    for factor in list(row.get("key_factors") or []):
        direction = str(factor.get("direction", ""))
        if direction == "supportive":
            supportive += 1
        elif direction == "drag":
            drag += 1
    return supportive, drag


def _attribute_prediction_row(row: Mapping[str, Any]) -> Dict[str, Any]:
    validation = dict(row.get("validation") or {})
    validation_status = str(validation.get("validation_status", ""))
    status = str(row.get("status", ""))
    downgrade_flags = {str(flag) for flag in list(row.get("downgrade_flags") or [])}
    direction = str(dict(row.get("prediction_value") or {}).get("expected_excess_direction", "neutral"))
    asset_return = _safe_float(validation.get("realized_return"))
    excess_return = _safe_float(validation.get("excess_return"))
    net_directional = _safe_float(validation.get("cost_adjusted_directional_return"))
    neutral_band = max(_safe_float(validation.get("neutral_band"), STRATEGY_V1_NEUTRAL_BAND), STRATEGY_V1_NEUTRAL_BAND)
    seed_score = _safe_float(row.get("seed_score"), 50.0)
    confidence_label = str(row.get("confidence_label", ""))
    supportive_count, drag_count = _factor_direction_counts(row)

    if status == "no_prediction":
        return {
            "status": "not_applicable",
            "label": "gated_out",
            "summary": "这个样本本来就被 strategy v1 门槛拒绝，不进入主归因。",
            "next_action": "先解决 universe / 流动性 / point-in-time 门槛，再谈策略有效性。",
            "severity": "info",
        }
    if validation_status == "pending_future_window":
        return {
            "status": "pending",
            "label": "pending_future_window",
            "summary": "未来 20 个交易日窗口还没走完，当前不能做后验归因。",
            "next_action": "等窗口结束后再跑 validate / attribute。",
            "severity": "info",
        }
    if validation_status != "validated":
        return {
            "status": "not_evaluable",
            "label": "data_not_evaluable",
            "summary": "当前样本缺少可验证的历史窗口或 benchmark 对照，无法做有效归因。",
            "next_action": "先补齐 point-in-time 数据，再重跑 replay / validate。",
            "severity": "warning",
        }
    if bool(validation.get("hit")):
        if net_directional <= 0:
            return {
                "status": "attributed",
                "label": "execution_cost_drag",
                "summary": "方向并不算错，但扣掉成本后边际收益被明显吃掉了。",
                "next_action": "收紧流动性门槛，或提高信号强度阈值再出手。",
                "severity": "warning",
            }
        return {
            "status": "attributed",
            "label": "confirmed_edge",
            "summary": "方向和相对收益都兑现了，当前更像保留为基线样本。",
            "next_action": "保留这类样本，后续拿来校准置信度分桶和 champion baseline。",
            "severity": "info",
        }

    if downgrade_flags & {"history_fallback_mode", "catalyst_coverage_degraded", "benchmark_history_missing"}:
        return {
            "status": "attributed",
            "label": "data_degradation_or_proxy_limit",
            "summary": "失败发生在降级/代理链路下，当前还不能直接把它认定为策略逻辑错误。",
            "next_action": "先补完整的 point-in-time 原始链路，再重跑历史验证。",
            "severity": "warning",
        }
    if direction in {"positive", "negative"}:
        if (direction == "positive" and asset_return > 0 and excess_return < 0) or (
            direction == "negative" and asset_return < 0 and excess_return > 0
        ):
            return {
                "status": "attributed",
                "label": "universe_bias",
                "summary": "标的绝对方向没有完全错，但 benchmark-relative 目标没跑赢，更多像相对收益目标设定问题。",
                "next_action": "增加行业/风格相对 benchmark 切片，别只看标的绝对涨跌。",
                "severity": "warning",
            }
    if abs(excess_return) <= max(neutral_band * 1.5, 0.03):
        return {
            "status": "attributed",
            "label": "horizon_mismatch",
            "summary": "20 日窗口里的结果更接近噪音或延迟兑现，当前更像周期错配而不是主逻辑被完全推翻。",
            "next_action": "补 5 / 60 日切片，检查当前 20 日 horizon 是否合适。",
            "severity": "warning",
        }
    if confidence_label == "低" or abs(seed_score - 50.0) <= 8.0 or (supportive_count >= 2 and drag_count >= 2):
        return {
            "status": "attributed",
            "label": "weight_misallocation",
            "summary": "支撑和拖累信号本来就混在一起，但当前权重仍把它推成单边预测，更像权重失衡。",
            "next_action": "先做权重实验，再考虑扩很多新因子。",
            "severity": "warning",
        }
    if abs(excess_return) >= 0.08:
        return {
            "status": "attributed",
            "label": "missing_factor",
            "summary": "结果和预测方向偏离明显，当前因子族很可能缺了关键解释变量。",
            "next_action": "优先补新的候选因子或更完整的事件/盈利代理，再重跑 replay。",
            "severity": "warning",
        }
    return {
        "status": "attributed",
        "label": "regime_shift",
        "summary": "这次失败更像市场环境切换，原来的权重在这一段窗口失灵。",
        "next_action": "按 regime 切片做 validate / experiment，避免把单一环境里的最优权重推广到所有阶段。",
        "severity": "warning",
    }


def _attribute_recommendations(label_counts: Mapping[str, int]) -> List[str]:
    recommendations: List[str] = []
    if int(label_counts.get("weight_misallocation", 0)) > 0:
        recommendations.append("`weight_misallocation` 偏多：下一轮优先跑 `strategy experiment` 比较 baseline / momentum_tilt / defensive_tilt。")
    if int(label_counts.get("missing_factor", 0)) > 0:
        recommendations.append("`missing_factor` 已出现：下一轮优先补候选因子池，而不是继续细调现有权重。")
    if int(label_counts.get("horizon_mismatch", 0)) > 0:
        recommendations.append("`horizon_mismatch` 偏多：先补 5 日 / 60 日验证切片，别急着宣判当前因子失效。")
    if int(label_counts.get("universe_bias", 0)) > 0:
        recommendations.append("`universe_bias` 已出现：要把相对基准和绝对方向拆开看，避免只因为个股上涨就误判相对收益目标。")
    if int(label_counts.get("data_degradation_or_proxy_limit", 0)) > 0:
        recommendations.append("`data_degradation_or_proxy_limit` 偏多：这批样本先不该拿来改权重，应优先补 point-in-time 数据链。")
    if int(label_counts.get("execution_cost_drag", 0)) > 0:
        recommendations.append("`execution_cost_drag` 已出现：需要收紧流动性门槛或提高出手阈值。")
    if not recommendations:
        recommendations.append("当前归因没有暴露明显结构性缺口，下一轮优先扩大 replay 样本而不是盲目改规则。")
    return recommendations


def _sorted_validated_rows(rows: Sequence[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    validated_rows = [
        dict(row)
        for row in rows
        if str(dict(row.get("validation") or {}).get("validation_status", "")) == "validated"
    ]
    validated_rows.sort(
        key=lambda row: (
            str(row.get("as_of", "")),
            str(row.get("created_at", "")),
            str(row.get("prediction_id", "")),
        )
    )
    return validated_rows


def _validated_metric_summary(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    validated_rows = _sorted_validated_rows(rows)
    if not validated_rows:
        return {
            "count": 0,
            "start_as_of": "—",
            "end_as_of": "—",
            "hit_rate": 0.0,
            "avg_excess_return": 0.0,
            "avg_cost_adjusted_directional_return": 0.0,
            "avg_max_drawdown": 0.0,
        }
    return {
        "count": len(validated_rows),
        "start_as_of": str(validated_rows[0].get("as_of", "—")),
        "end_as_of": str(validated_rows[-1].get("as_of", "—")),
        "hit_rate": sum(1 for row in validated_rows if bool(dict(row.get("validation") or {}).get("hit"))) / len(validated_rows),
        "avg_excess_return": sum(_safe_float(dict(row.get("validation") or {}).get("excess_return")) for row in validated_rows)
        / len(validated_rows),
        "avg_cost_adjusted_directional_return": sum(
            _safe_float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return")) for row in validated_rows
        )
        / len(validated_rows),
        "avg_max_drawdown": sum(_safe_float(dict(row.get("validation") or {}).get("max_drawdown")) for row in validated_rows) / len(validated_rows),
    }


def _out_of_sample_validation(
    rows: Sequence[Mapping[str, Any]],
    *,
    overlap_fixture_summary: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    validated_rows = _sorted_validated_rows(rows)
    overlap_summary = dict(overlap_fixture_summary or {})
    blockers: List[str] = []
    if int(overlap_summary.get("violation_count") or 0) > 0:
        blockers.append("overlap_fixture_blocked")
    if not validated_rows:
        blockers.append("no_validated_rows")
    elif len(validated_rows) < STRATEGY_V1_OUT_OF_SAMPLE_MIN_VALIDATED_ROWS:
        blockers.append("validated_rows_below_floor")

    holdout_size = max(STRATEGY_V1_OUT_OF_SAMPLE_MIN_HOLDOUT_ROWS, len(validated_rows) // 3) if validated_rows else 0
    if validated_rows and len(validated_rows) - holdout_size < STRATEGY_V1_OUT_OF_SAMPLE_MIN_DEVELOPMENT_ROWS:
        holdout_size = max(len(validated_rows) - STRATEGY_V1_OUT_OF_SAMPLE_MIN_DEVELOPMENT_ROWS, 0)
    development_rows = validated_rows[:-holdout_size] if holdout_size > 0 else list(validated_rows)
    holdout_rows = validated_rows[-holdout_size:] if holdout_size > 0 else []
    if validated_rows and len(development_rows) < STRATEGY_V1_OUT_OF_SAMPLE_MIN_DEVELOPMENT_ROWS:
        blockers.append("development_rows_below_floor")
    if validated_rows and len(holdout_rows) < STRATEGY_V1_OUT_OF_SAMPLE_MIN_HOLDOUT_ROWS:
        blockers.append("holdout_rows_below_floor")

    development_metrics = _validated_metric_summary(development_rows)
    holdout_metrics = _validated_metric_summary(holdout_rows)
    hit_rate_delta = holdout_metrics["hit_rate"] - development_metrics["hit_rate"]
    avg_excess_return_delta = holdout_metrics["avg_excess_return"] - development_metrics["avg_excess_return"]
    avg_net_directional_return_delta = (
        holdout_metrics["avg_cost_adjusted_directional_return"] - development_metrics["avg_cost_adjusted_directional_return"]
    )
    avg_max_drawdown_delta = holdout_metrics["avg_max_drawdown"] - development_metrics["avg_max_drawdown"]
    decision_reasons: List[str] = []

    if blockers:
        status = "blocked"
        summary = "当前 validated rows 还不足以形成可信的 development / holdout 切片，out-of-sample validate 先阻断。"
        next_action = "继续积累 non-overlap validated rows，至少先把 development / holdout 两段都补到最低样本要求。"
    else:
        if hit_rate_delta <= STRATEGY_V1_OUT_OF_SAMPLE_MAX_HIT_RATE_REGRESSION:
            decision_reasons.append("holdout_hit_rate_regressed")
        if avg_excess_return_delta <= STRATEGY_V1_OUT_OF_SAMPLE_MAX_AVG_EXCESS_REGRESSION:
            decision_reasons.append("holdout_avg_excess_regressed")
        if avg_net_directional_return_delta <= STRATEGY_V1_OUT_OF_SAMPLE_MAX_AVG_NET_REGRESSION:
            decision_reasons.append("holdout_avg_net_regressed")
        if holdout_metrics["avg_excess_return"] < 0:
            decision_reasons.append("holdout_avg_excess_negative")
        if holdout_metrics["avg_cost_adjusted_directional_return"] < 0:
            decision_reasons.append("holdout_avg_net_negative")

        if decision_reasons:
            status = "watchlist"
            summary = "最新 holdout 相比 development 已经出现明显退化，out-of-sample validate 当前转入 watchlist。"
            next_action = "先别把 aggregate 平均值当成稳定结论，优先扩最近样本并检查最新 cohort 是否发生 regime 漂移。"
        else:
            status = "stable"
            summary = "当前 holdout 没有出现明显退化，out-of-sample validate 暂时维持稳定。"
            next_action = "继续滚动更新 holdout，避免只凭一段时间窗就宣称稳定。"

    return {
        "status": status,
        "required_validated_rows": STRATEGY_V1_OUT_OF_SAMPLE_MIN_VALIDATED_ROWS,
        "required_development_rows": STRATEGY_V1_OUT_OF_SAMPLE_MIN_DEVELOPMENT_ROWS,
        "required_holdout_rows": STRATEGY_V1_OUT_OF_SAMPLE_MIN_HOLDOUT_ROWS,
        "validated_rows": len(validated_rows),
        "development_metrics": development_metrics,
        "holdout_metrics": holdout_metrics,
        "holdout_start_as_of": holdout_metrics.get("start_as_of", "—"),
        "holdout_end_as_of": holdout_metrics.get("end_as_of", "—"),
        "hit_rate_delta": hit_rate_delta,
        "avg_excess_return_delta": avg_excess_return_delta,
        "avg_cost_adjusted_directional_return_delta": avg_net_directional_return_delta,
        "avg_max_drawdown_delta": avg_max_drawdown_delta,
        "blockers": blockers,
        "decision_reasons": decision_reasons,
        "summary": summary,
        "next_action": next_action,
    }


def _chronological_cohort_validation(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    validated_rows = _sorted_validated_rows(rows)
    if not validated_rows:
        return {
            "status": "blocked",
            "cohort_rows": [],
            "blockers": ["no_validated_rows"],
            "summary": "当前还没有 validated rows，无法做 chronological cohort 对比。",
        }
    if len(validated_rows) < 3:
        return {
            "status": "blocked",
            "cohort_rows": [],
            "blockers": ["validated_rows_below_floor"],
            "summary": "validated rows 还不足以拆出 earliest / middle / latest cohort。",
        }

    cohort_count = min(3, len(validated_rows))
    labels = ["earliest", "middle", "latest"] if cohort_count >= 3 else ["earliest", "latest"]
    base_size = len(validated_rows) // cohort_count
    remainder = len(validated_rows) % cohort_count
    start_index = 0
    cohort_rows: List[Dict[str, Any]] = []
    for index in range(cohort_count):
        size = base_size + (1 if index < remainder else 0)
        cohort_slice = validated_rows[start_index : start_index + size]
        start_index += size
        metrics = _validated_metric_summary(cohort_slice)
        cohort_rows.append(
            {
                "label": labels[index],
                "start_as_of": metrics.get("start_as_of", "—"),
                "end_as_of": metrics.get("end_as_of", "—"),
                "count": metrics.get("count", 0),
                "hit_rate": metrics.get("hit_rate", 0.0),
                "avg_excess_return": metrics.get("avg_excess_return", 0.0),
                "avg_cost_adjusted_directional_return": metrics.get("avg_cost_adjusted_directional_return", 0.0),
                "avg_max_drawdown": metrics.get("avg_max_drawdown", 0.0),
            }
        )

    earliest_row = cohort_rows[0]
    latest_row = cohort_rows[-1]
    hit_rate_delta = _safe_float(latest_row.get("hit_rate")) - _safe_float(earliest_row.get("hit_rate"))
    avg_excess_return_delta = _safe_float(latest_row.get("avg_excess_return")) - _safe_float(earliest_row.get("avg_excess_return"))
    avg_net_directional_return_delta = _safe_float(latest_row.get("avg_cost_adjusted_directional_return")) - _safe_float(
        earliest_row.get("avg_cost_adjusted_directional_return")
    )
    if (
        hit_rate_delta <= STRATEGY_V1_OUT_OF_SAMPLE_MAX_HIT_RATE_REGRESSION
        or avg_excess_return_delta <= STRATEGY_V1_OUT_OF_SAMPLE_MAX_AVG_EXCESS_REGRESSION
        or avg_net_directional_return_delta <= STRATEGY_V1_OUT_OF_SAMPLE_MAX_AVG_NET_REGRESSION
    ):
        status = "watchlist"
        summary = "latest cohort 相比 earliest cohort 已经出现明显退化，需要结合 regime / out-of-sample 一起看。"
    else:
        status = "stable"
        summary = "earliest -> latest cohort 暂时没看到明显断崖式退化，但样本仍然偏窄。"

    return {
        "status": status,
        "cohort_rows": cohort_rows,
        "hit_rate_delta_latest_vs_earliest": hit_rate_delta,
        "avg_excess_return_delta_latest_vs_earliest": avg_excess_return_delta,
        "avg_cost_adjusted_directional_return_delta_latest_vs_earliest": avg_net_directional_return_delta,
        "blockers": [],
        "summary": summary,
    }


def _cross_sectional_validation(rows: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    validated_rows = _sorted_validated_rows(rows)
    blockers: List[str] = []
    if not validated_rows:
        blockers.append("no_validated_rows")
        return {
            "status": "blocked",
            "cohort_count": 0,
            "eligible_symbol_count": 0,
            "eligible_cohort_rows": [],
            "blockers": blockers,
            "summary": "当前还没有 validated rows，无法做 cross-sectional validate。",
        }

    grouped: Dict[str, List[Dict[str, Any]]] = {}
    for row in validated_rows:
        grouped.setdefault(str(row.get("as_of", "")), []).append(dict(row))

    cohort_rows: List[Dict[str, Any]] = []
    eligible_symbols: set[str] = set()
    for as_of, cohort in sorted(grouped.items()):
        symbol_count = len({str(row.get("symbol", "")).strip() for row in cohort if str(row.get("symbol", "")).strip()})
        if symbol_count < STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORT_SYMBOLS:
            continue
        frame = pd.DataFrame(
            [
                {
                    "symbol": str(row.get("symbol", "")),
                    "seed_score": _safe_float(row.get("seed_score")),
                    "excess_return": _safe_float(dict(row.get("validation") or {}).get("excess_return")),
                    "net_directional_return": _safe_float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return")),
                }
                for row in cohort
            ]
        ).drop_duplicates(subset=["symbol"], keep="last")
        if len(frame) < STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORT_SYMBOLS:
            continue
        if frame["seed_score"].nunique() <= 1 or frame["excess_return"].nunique() <= 1:
            rank_corr = 0.0
        else:
            rank_corr = _safe_float(frame["seed_score"].rank(method="average").corr(frame["excess_return"].rank(method="average"), method="pearson"))
        sorted_frame = frame.sort_values("seed_score", ascending=False).reset_index(drop=True)
        bucket_size = max(len(sorted_frame) // 3, 1)
        top_slice = sorted_frame.head(bucket_size)
        bottom_slice = sorted_frame.tail(bucket_size)
        top_avg_excess_return = _safe_float(top_slice["excess_return"].mean())
        bottom_avg_excess_return = _safe_float(bottom_slice["excess_return"].mean())
        top_bottom_spread = top_avg_excess_return - bottom_avg_excess_return
        top_avg_net_directional_return = _safe_float(top_slice["net_directional_return"].mean())
        bottom_avg_net_directional_return = _safe_float(bottom_slice["net_directional_return"].mean())
        top_bottom_net_spread = top_avg_net_directional_return - bottom_avg_net_directional_return
        eligible_symbols.update(str(symbol) for symbol in list(frame["symbol"]) if str(symbol).strip())
        cohort_rows.append(
            {
                "as_of": as_of,
                "symbol_count": int(len(frame)),
                "rank_corr": rank_corr,
                "top_avg_excess_return": top_avg_excess_return,
                "bottom_avg_excess_return": bottom_avg_excess_return,
                "top_bottom_spread": top_bottom_spread,
                "top_avg_net_directional_return": top_avg_net_directional_return,
                "bottom_avg_net_directional_return": bottom_avg_net_directional_return,
                "top_bottom_net_spread": top_bottom_net_spread,
            }
        )

    if len(cohort_rows) < STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORTS:
        blockers.append("cross_sectional_cohorts_below_floor")
    if len(eligible_symbols) < STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORT_SYMBOLS:
        blockers.append("cross_sectional_symbols_below_floor")

    avg_rank_corr = sum(_safe_float(row.get("rank_corr")) for row in cohort_rows) / len(cohort_rows) if cohort_rows else 0.0
    avg_top_bottom_spread = sum(_safe_float(row.get("top_bottom_spread")) for row in cohort_rows) / len(cohort_rows) if cohort_rows else 0.0
    avg_top_bottom_net_spread = sum(_safe_float(row.get("top_bottom_net_spread")) for row in cohort_rows) / len(cohort_rows) if cohort_rows else 0.0
    positive_rank_corr_count = sum(1 for row in cohort_rows if _safe_float(row.get("rank_corr")) > 0)
    positive_spread_count = sum(1 for row in cohort_rows if _safe_float(row.get("top_bottom_spread")) > 0)
    decision_reasons: List[str] = []

    if blockers:
        status = "blocked"
        summary = "当前账本里还没有足够的同日多标的 cohort，cross-sectional validate 先阻断。"
        next_action = "先补多标的 replay / validate 样本，再用同日 cohort 检查 seed score 是否真的对应更高超额收益。"
    else:
        if avg_rank_corr < STRATEGY_V1_CROSS_SECTIONAL_MIN_AVG_RANK_CORR:
            decision_reasons.append("avg_rank_corr_too_low")
        if avg_top_bottom_spread < STRATEGY_V1_CROSS_SECTIONAL_MIN_AVG_TOP_BOTTOM_SPREAD:
            decision_reasons.append("avg_top_bottom_spread_too_low")
        if positive_rank_corr_count < max((len(cohort_rows) + 1) // 2, 1):
            decision_reasons.append("positive_rank_corr_cohorts_too_few")
        if positive_spread_count < max((len(cohort_rows) + 1) // 2, 1):
            decision_reasons.append("positive_spread_cohorts_too_few")

        if decision_reasons:
            status = "watchlist"
            summary = "同日多标的 cohort 已经具备，但横截面 rank 质量还不稳定，先进入 watchlist。"
            next_action = "继续扩大同日 cohort，并重点看高 score 组是否持续跑赢低 score 组。"
        else:
            status = "stable"
            summary = "当前同日多标的 cohort 里，高 score 组整体能跑赢低 score 组，cross-sectional validate 暂时稳定。"
            next_action = "继续滚动追加 cohort，避免只凭少数日期就宣称横截面稳定。"

    return {
        "status": status,
        "required_cohort_symbols": STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORT_SYMBOLS,
        "required_cohorts": STRATEGY_V1_CROSS_SECTIONAL_MIN_COHORTS,
        "cohort_count": len(cohort_rows),
        "eligible_symbol_count": len(eligible_symbols),
        "avg_rank_corr": avg_rank_corr,
        "avg_top_bottom_spread": avg_top_bottom_spread,
        "avg_top_bottom_net_spread": avg_top_bottom_net_spread,
        "positive_rank_corr_count": positive_rank_corr_count,
        "positive_spread_count": positive_spread_count,
        "eligible_cohort_rows": cohort_rows[:10],
        "blockers": blockers,
        "decision_reasons": decision_reasons,
        "summary": summary,
        "next_action": next_action,
    }


def _rollback_gate(
    rows: Sequence[Mapping[str, Any]],
    *,
    overlap_fixture_summary: Mapping[str, Any] | None = None,
    current_label: str = "current_batch",
) -> Dict[str, Any]:
    overlap_summary = dict(overlap_fixture_summary or {})
    evaluated_rows = [
        dict(row)
        for row in rows
        if str(dict(row.get("validation") or {}).get("validation_status", "")) == "validated"
    ]
    hit_rate = (
        sum(1 for row in evaluated_rows if bool(dict(row.get("validation") or {}).get("hit"))) / len(evaluated_rows)
        if evaluated_rows
        else 0.0
    )
    avg_excess_return = (
        sum(_safe_float(dict(row.get("validation") or {}).get("excess_return")) for row in evaluated_rows) / len(evaluated_rows)
        if evaluated_rows
        else 0.0
    )
    avg_net_directional_return = (
        sum(_safe_float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return")) for row in evaluated_rows)
        / len(evaluated_rows)
        if evaluated_rows
        else 0.0
    )
    label_counts: Dict[str, int] = {}
    for row in evaluated_rows:
        attribution = dict(row.get("attribution") or _attribute_prediction_row(row))
        label = str(attribution.get("label", "")).strip() or "unlabeled"
        label_counts[label] = label_counts.get(label, 0) + 1
    structural_miss_count = sum(
        int(label_counts.get(label, 0))
        for label in ("weight_misallocation", "missing_factor", "regime_shift", "universe_bias")
    )
    degraded_miss_count = int(label_counts.get("data_degradation_or_proxy_limit", 0))
    execution_cost_drag_count = int(label_counts.get("execution_cost_drag", 0))
    horizon_mismatch_count = int(label_counts.get("horizon_mismatch", 0))
    confirmed_edge_count = int(label_counts.get("confirmed_edge", 0))
    structural_miss_share = structural_miss_count / len(evaluated_rows) if evaluated_rows else 0.0
    blockers: List[str] = []
    if int(overlap_summary.get("violation_count") or 0) > 0:
        blockers.append("overlap_fixture_blocked")
    if not evaluated_rows:
        blockers.append("no_validated_rows")
    elif len(evaluated_rows) < STRATEGY_V1_ROLLBACK_MIN_VALIDATED_ROWS:
        blockers.append("validated_rows_below_floor")

    if blockers:
        status = "blocked"
        summary = "当前 rollback gate 还没到可裁决状态，先别把这批样本当成当前基线方案的定论。"
        next_action = "先补更多 non-overlap validated rows，再决定是 hold、watch 还是进入 rollback 讨论。"
    elif (
        hit_rate < STRATEGY_V1_ROLLBACK_TRIGGER_HIT_RATE
        and avg_excess_return <= STRATEGY_V1_ROLLBACK_TRIGGER_AVG_EXCESS_RETURN
        and avg_net_directional_return <= STRATEGY_V1_ROLLBACK_TRIGGER_AVG_NET_DIRECTIONAL_RETURN
        and structural_miss_share >= STRATEGY_V1_ROLLBACK_TRIGGER_STRUCTURAL_MISS_SHARE
        and structural_miss_count > 0
    ):
        status = "rollback_candidate"
        summary = "当前 validated 样本里结构性 miss 已经占主导，当前基线方案应进入 rollback 候选讨论。"
        next_action = "冻结继续宣称它是稳定 champion，优先扩样本、切 regime，并准备 rollback review。"
    elif (
        hit_rate < STRATEGY_V1_ROLLBACK_WATCH_HIT_RATE
        or avg_excess_return < 0
        or avg_net_directional_return < 0
        or structural_miss_share >= STRATEGY_V1_ROLLBACK_WATCH_STRUCTURAL_MISS_SHARE
        or execution_cost_drag_count > 0
    ):
        status = "watchlist"
        summary = "当前 baseline 已经出现持续压力，但还没到直接 rollback 的强结论。"
        next_action = "继续扩大 non-overlap validated rows，并重点看结构性 miss 和执行拖累是否持续。"
    else:
        status = "hold"
        summary = "当前 baseline 还维持在可 hold 区间，暂时没有进入 rollback 讨论。"
        next_action = "继续累积 validated rows，把 rollback gate 当作常规健康检查，而不是一次性结论。"

    return {
        "status": status,
        "current_label": str(current_label).strip() or "current_batch",
        "required_validated_rows": STRATEGY_V1_ROLLBACK_MIN_VALIDATED_ROWS,
        "validated_rows": len(evaluated_rows),
        "overlap_violation_count": int(overlap_summary.get("violation_count") or 0),
        "hit_rate": hit_rate,
        "avg_excess_return": avg_excess_return,
        "avg_cost_adjusted_directional_return": avg_net_directional_return,
        "structural_miss_count": structural_miss_count,
        "structural_miss_share": structural_miss_share,
        "degraded_miss_count": degraded_miss_count,
        "execution_cost_drag_count": execution_cost_drag_count,
        "horizon_mismatch_count": horizon_mismatch_count,
        "confirmed_edge_count": confirmed_edge_count,
        "blockers": blockers,
        "summary": summary,
        "next_action": next_action,
    }


def attribute_strategy_rows(rows: Sequence[Mapping[str, Any]]) -> Tuple[List[Dict[str, Any]], Dict[str, Any]]:
    updated_rows: List[Dict[str, Any]] = []
    attributed_rows: List[Dict[str, Any]] = []
    label_counts: Dict[str, int] = {}

    for row in rows:
        cloned = dict(row)
        attribution = _attribute_prediction_row(cloned)
        cloned["attribution"] = attribution
        updated_rows.append(cloned)
        label = str(attribution.get("label", ""))
        label_counts[label] = label_counts.get(label, 0) + 1
        if str(attribution.get("status", "")) == "attributed":
            attributed_rows.append(cloned)

    label_rows: List[Dict[str, Any]] = []
    for label, count in sorted(label_counts.items(), key=lambda item: (-item[1], item[0])):
        rows_for_label = [row for row in updated_rows if str(dict(row.get("attribution") or {}).get("label", "")) == label]
        validated_rows = [row for row in rows_for_label if str(dict(row.get("validation") or {}).get("validation_status", "")) == "validated"]
        hit_rate = (
            sum(1 for row in validated_rows if bool(dict(row.get("validation") or {}).get("hit"))) / len(validated_rows)
            if validated_rows
            else 0.0
        )
        avg_excess = (
            sum(_safe_float(dict(row.get("validation") or {}).get("excess_return")) for row in validated_rows) / len(validated_rows)
            if validated_rows
            else 0.0
        )
        avg_net = (
            sum(_safe_float(dict(row.get("validation") or {}).get("cost_adjusted_directional_return")) for row in validated_rows) / len(validated_rows)
            if validated_rows
            else 0.0
        )
        label_rows.append(
            {
                "label": label,
                "count": count,
                "share": count / len(updated_rows) if updated_rows else 0.0,
                "hit_rate": hit_rate,
                "avg_excess_return": avg_excess,
                "avg_net_directional_return": avg_net,
            }
        )

    recent_rows = []
    for row in updated_rows[:10]:
        attribution = dict(row.get("attribution") or {})
        validation = dict(row.get("validation") or {})
        recent_rows.append(
            {
                "as_of": str(row.get("as_of", "")),
                "symbol": str(row.get("symbol", "")),
                "label": str(attribution.get("label", "")),
                "summary": str(attribution.get("summary", "")),
                "next_action": str(attribution.get("next_action", "")),
                "excess_return": _safe_float(validation.get("excess_return")),
                "hit": bool(validation.get("hit")),
                "status": str(attribution.get("status", "")),
            }
        )

    summary = {
        "total_rows": len(updated_rows),
        "attributed_rows": len(attributed_rows),
        "pending_rows": sum(1 for row in updated_rows if str(dict(row.get("attribution") or {}).get("label", "")) == "pending_future_window"),
        "not_applicable_rows": sum(1 for row in updated_rows if str(dict(row.get("attribution") or {}).get("status", "")) == "not_applicable"),
        "label_rows": label_rows,
        "recent_rows": recent_rows,
        "recommendations": _attribute_recommendations(label_counts),
        "notes": [
            "当前 attribution 还是 v1 窄标签集，重点先区分权重失衡、缺因子、周期错配、数据降级和执行拖累。",
            "它的目标不是一次讲完所有故事，而是给下一轮 experiment / factor backlog 一个明确起点。",
        ],
    }
    return updated_rows, summary


def _experiment_primary_score(summary: Mapping[str, Any]) -> float:
    return (
        _safe_float(summary.get("avg_excess_return")) * 100.0
        + _safe_float(summary.get("avg_cost_adjusted_directional_return")) * 60.0
        + _safe_float(summary.get("hit_rate")) * 10.0
        + _safe_float(summary.get("avg_max_drawdown")) * 20.0
    )


def _promotion_gate(
    variant_rows: Sequence[Mapping[str, Any]],
    *,
    overlap_fixture_summary: Mapping[str, Any] | None = None,
    sample_count: int = 0,
    require_cross_sectional: bool = False,
) -> Dict[str, Any]:
    ordered_rows = [dict(row) for row in variant_rows if dict(row)]
    baseline_row = next((row for row in ordered_rows if str(row.get("variant", "")) == "baseline"), None)
    candidate_row = next((row for row in ordered_rows if str(row.get("variant", "")) != "baseline"), None)
    champion_row = ordered_rows[0] if ordered_rows else None
    blockers: List[str] = []
    decision_reasons: List[str] = []
    overlap_summary = dict(overlap_fixture_summary or {})
    required_validated_rows = STRATEGY_V1_PROMOTION_MIN_VALIDATED_ROWS
    baseline_out_of_sample_status = str(baseline_row.get("out_of_sample_status", "")) if baseline_row else ""
    candidate_out_of_sample_status = str(candidate_row.get("out_of_sample_status", "")) if candidate_row else ""
    baseline_cross_sectional_status = str(baseline_row.get("cross_sectional_status", "")) if baseline_row else ""
    candidate_cross_sectional_status = str(candidate_row.get("cross_sectional_status", "")) if candidate_row else ""
    if not ordered_rows:
        blockers.append("variant_rows_missing")
    if not baseline_row:
        blockers.append("baseline_missing")
    if int(overlap_summary.get("violation_count") or 0) > 0:
        blockers.append("overlap_fixture_blocked")
    if max(int(sample_count), 0) < required_validated_rows:
        blockers.append("sample_count_below_floor")
    if baseline_row and int(baseline_row.get("validated_sample_count") or 0) < required_validated_rows:
        blockers.append("baseline_validated_rows_below_floor")
    if candidate_row and int(candidate_row.get("validated_sample_count") or 0) < required_validated_rows:
        blockers.append("candidate_validated_rows_below_floor")
    if baseline_row and baseline_out_of_sample_status == "blocked":
        blockers.append("baseline_out_of_sample_blocked")
    if candidate_row and candidate_out_of_sample_status == "blocked":
        blockers.append("candidate_out_of_sample_blocked")
    if require_cross_sectional and baseline_row and baseline_cross_sectional_status == "blocked":
        blockers.append("baseline_cross_sectional_blocked")
    if require_cross_sectional and candidate_row and candidate_cross_sectional_status == "blocked":
        blockers.append("candidate_cross_sectional_blocked")

    primary_score_delta = 0.0
    hit_rate_delta = 0.0
    avg_excess_return_delta = 0.0
    avg_net_directional_return_delta = 0.0
    avg_max_drawdown_delta = 0.0
    holdout_avg_excess_return_delta = 0.0
    holdout_avg_cost_adjusted_directional_return_delta = 0.0
    if baseline_row and candidate_row:
        primary_score_delta = _safe_float(candidate_row.get("primary_score")) - _safe_float(baseline_row.get("primary_score"))
        hit_rate_delta = _safe_float(candidate_row.get("hit_rate")) - _safe_float(baseline_row.get("hit_rate"))
        avg_excess_return_delta = _safe_float(candidate_row.get("avg_excess_return")) - _safe_float(baseline_row.get("avg_excess_return"))
        avg_net_directional_return_delta = _safe_float(candidate_row.get("avg_cost_adjusted_directional_return")) - _safe_float(
            baseline_row.get("avg_cost_adjusted_directional_return")
        )
        avg_max_drawdown_delta = _safe_float(candidate_row.get("avg_max_drawdown")) - _safe_float(baseline_row.get("avg_max_drawdown"))
        holdout_avg_excess_return_delta = _safe_float(candidate_row.get("holdout_avg_excess_return")) - _safe_float(
            baseline_row.get("holdout_avg_excess_return")
        )
        holdout_avg_cost_adjusted_directional_return_delta = _safe_float(
            candidate_row.get("holdout_avg_cost_adjusted_directional_return")
        ) - _safe_float(baseline_row.get("holdout_avg_cost_adjusted_directional_return"))

    if blockers:
        status = "blocked"
        summary = "当前 experiment 还没满足窄版 promotion gate 的最低数据合同。"
        next_action = "先补足 non-overlap validated rows 并清掉 overlap blocker，再比较 champion / challenger。"
    elif not candidate_row:
        status = "stay_on_baseline"
        summary = "当前没有比 baseline 更值得推进的 challenger，先保留 baseline。"
        next_action = "继续维护 baseline，并等待新的 challenger 或更严格样本。"
        decision_reasons.append("challenger_missing")
    else:
        if champion_row and baseline_row and str(champion_row.get("variant", "")) == "baseline":
            decision_reasons.append("baseline_still_best")
        if candidate_out_of_sample_status != "stable":
            decision_reasons.append("candidate_out_of_sample_not_stable")
        if require_cross_sectional and candidate_cross_sectional_status != "stable":
            decision_reasons.append("candidate_cross_sectional_not_stable")
        if primary_score_delta < STRATEGY_V1_PROMOTION_MIN_PRIMARY_SCORE_DELTA:
            decision_reasons.append("primary_score_edge_too_small")
        if hit_rate_delta < 0:
            decision_reasons.append("hit_rate_not_improved")
        if avg_excess_return_delta < STRATEGY_V1_PROMOTION_MIN_AVG_EXCESS_RETURN_DELTA:
            decision_reasons.append("avg_excess_return_not_improved")
        if avg_net_directional_return_delta < STRATEGY_V1_PROMOTION_MIN_AVG_NET_DIRECTIONAL_RETURN_DELTA:
            decision_reasons.append("avg_cost_adjusted_return_not_improved")
        if avg_max_drawdown_delta < STRATEGY_V1_PROMOTION_MAX_DRAWDOWN_REGRESSION:
            decision_reasons.append("drawdown_regressed")
        if holdout_avg_excess_return_delta < 0:
            decision_reasons.append("holdout_avg_excess_not_improved")
        if holdout_avg_cost_adjusted_directional_return_delta < 0:
            decision_reasons.append("holdout_avg_net_not_improved")

        if decision_reasons:
            status = "stay_on_baseline"
            summary = "当前最佳 challenger 还没有稳定跑赢 baseline，promotion gate 先保持 baseline。"
            next_action = "challenger 继续留在研究观察区，先扩大样本并做更严格 validate，再考虑下一阶段。"
        else:
            status = "queue_for_next_stage"
            summary = "当前最佳 challenger 已通过窄版 promotion gate，可进入下一阶段验证。"
            next_action = "进入更严格的 out-of-sample / cohort / cross-sectional validate，并安排外审；仍不能直接切换正式执行口径。"

    return {
        "status": status,
        "baseline_variant": str(baseline_row.get("variant", "")) if baseline_row else "",
        "champion_variant": str(champion_row.get("variant", "")) if champion_row else "",
        "candidate_variant": str(candidate_row.get("variant", "")) if candidate_row else "",
        "required_validated_rows": required_validated_rows,
        "sample_count": max(int(sample_count), 0),
        "baseline_validated_rows": int(baseline_row.get("validated_sample_count") or 0) if baseline_row else 0,
        "candidate_validated_rows": int(candidate_row.get("validated_sample_count") or 0) if candidate_row else 0,
        "baseline_out_of_sample_status": baseline_out_of_sample_status,
        "candidate_out_of_sample_status": candidate_out_of_sample_status,
        "baseline_cross_sectional_status": baseline_cross_sectional_status,
        "candidate_cross_sectional_status": candidate_cross_sectional_status,
        "primary_score_delta": round(primary_score_delta, 4),
        "hit_rate_delta": round(hit_rate_delta, 4),
        "avg_excess_return_delta": round(avg_excess_return_delta, 4),
        "avg_cost_adjusted_directional_return_delta": round(avg_net_directional_return_delta, 4),
        "avg_max_drawdown_delta": round(avg_max_drawdown_delta, 4),
        "holdout_avg_excess_return_delta": round(holdout_avg_excess_return_delta, 4),
        "holdout_avg_cost_adjusted_directional_return_delta": round(holdout_avg_cost_adjusted_directional_return_delta, 4),
        "blockers": blockers,
        "decision_reasons": decision_reasons,
        "production_ready": False,
        "summary": summary,
        "next_action": next_action,
    }


def generate_strategy_experiment(
    symbol: str,
    config: Mapping[str, Any],
    *,
    start: str = "",
    end: str = "",
    asset_gap_days: int = STRATEGY_V1_ASSET_GAP_DAYS,
    max_samples: int = 12,
    variants: Sequence[str] | None = None,
    batch_context: Mapping[str, Any] | None = None,
    cohort_recipe: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    asset_type = detect_asset_type(symbol, config)
    asset_history = _safe_normalize_history(fetch_asset_history(symbol, asset_type, dict(config)))
    benchmark_history = _safe_normalize_history(fetch_asset_history(STRATEGY_V1_BENCHMARK_SYMBOL, "cn_index", dict(config)))
    if asset_history.empty:
        raise ValueError(f"无法生成 strategy experiment，缺少 {symbol} 的完整日线。")
    start_stamp, end_stamp, sample_indices = _replay_sample_indices(
        asset_history,
        start=start,
        end=end,
        asset_gap_days=asset_gap_days,
        max_samples=max_samples,
    )
    variant_names = [str(item).strip() for item in (variants or ["baseline", "momentum_tilt", "defensive_tilt", "confirmation_tilt"]) if str(item).strip()]
    variant_rows: List[Dict[str, Any]] = []
    variant_validated_rows: Dict[str, List[Dict[str, Any]]] = {}
    benchmark_fixture_summary: Dict[str, Any] = {}
    overlap_fixture_summary: Dict[str, Any] = {}

    for variant_name in variant_names:
        variant = dict(STRATEGY_V1_EXPERIMENT_VARIANTS.get(variant_name) or {})
        if not variant:
            raise ValueError(f"未知 experiment variant: {variant_name}")
        replay_rows: List[Dict[str, Any]] = []
        for index in sample_indices:
            asset_slice = asset_history.iloc[: index + 1].copy()
            as_of = pd.Timestamp(asset_slice["date"].iloc[-1])
            benchmark_slice = benchmark_history[benchmark_history["date"] <= as_of].copy()
            replay_rows.append(
                _build_replay_prediction(
                    symbol=symbol,
                    name=str(symbol),
                    asset_type=asset_type,
                    asset_history=asset_slice,
                    benchmark_history=benchmark_slice,
                    note=f"experiment variant={variant_name}",
                    weight_scheme=dict(variant.get("weight_scheme") or {}),
                    factor_version=f"{STRATEGY_V1_REPLAY_FACTOR_VERSION}:{variant_name}",
                    prediction_mode="historical_experiment_v1",
                    experiment_variant=variant_name,
                    asset_reentry_gap_days=asset_gap_days,
                )
            )
        replay_rows, overlap_fixture_summary = _attach_overlap_fixtures(
            replay_rows,
            lambda _symbol, _asset_type: asset_history,
        )
        validated_rows, validation_summary = validate_strategy_rows(replay_rows, config)
        variant_validated_rows[variant_name] = list(validated_rows)
        _, attribution_summary = attribute_strategy_rows(validated_rows)
        if not benchmark_fixture_summary:
            benchmark_fixture_summary = _aggregate_benchmark_fixtures(replay_rows)
        out_of_sample_validation = dict(validation_summary.get("out_of_sample_validation") or {})
        dominant_label = ""
        dominant_count = 0
        if attribution_summary.get("label_rows"):
            dominant = list(attribution_summary.get("label_rows") or [])[0]
            dominant_label = str(dominant.get("label", ""))
            dominant_count = int(dominant.get("count", 0))
        variant_rows.append(
            {
                "variant": variant_name,
                "hypothesis": str(variant.get("hypothesis", "")),
                "sample_count": len(replay_rows),
                "validated_sample_count": int(validation_summary.get("validated_rows", 0)),
                "pending_sample_count": int(validation_summary.get("pending_rows", 0)),
                "out_of_sample_status": str(out_of_sample_validation.get("status", "")),
                "cross_sectional_status": str(dict(validation_summary.get("cross_sectional_validation") or {}).get("status", "")),
                "cross_sectional_avg_rank_corr": _safe_float(dict(validation_summary.get("cross_sectional_validation") or {}).get("avg_rank_corr")),
                "holdout_rows": int(dict(out_of_sample_validation.get("holdout_metrics") or {}).get("count", 0)),
                "holdout_avg_excess_return": _safe_float(dict(out_of_sample_validation.get("holdout_metrics") or {}).get("avg_excess_return")),
                "holdout_avg_cost_adjusted_directional_return": _safe_float(
                    dict(out_of_sample_validation.get("holdout_metrics") or {}).get("avg_cost_adjusted_directional_return")
                ),
                "hit_rate": _safe_float(validation_summary.get("hit_rate")),
                "avg_excess_return": _safe_float(validation_summary.get("avg_excess_return")),
                "avg_cost_adjusted_directional_return": _safe_float(validation_summary.get("avg_cost_adjusted_directional_return")),
                "avg_max_drawdown": _safe_float(validation_summary.get("avg_max_drawdown")),
                "dominant_attribution": dominant_label,
                "dominant_attribution_count": dominant_count,
                "primary_score": _experiment_primary_score(validation_summary),
            }
        )

    variant_rows.sort(key=lambda row: (float(row.get("primary_score", 0.0)), float(row.get("avg_excess_return", 0.0))), reverse=True)
    baseline_row = next((row for row in variant_rows if str(row.get("variant", "")) == "baseline"), None)
    champion_row = variant_rows[0] if variant_rows else None
    challenger_row = next((row for row in variant_rows if str(row.get("variant", "")) != "baseline"), None)
    promotion_gate = _promotion_gate(
        variant_rows,
        overlap_fixture_summary=overlap_fixture_summary,
        sample_count=len(sample_indices),
    )
    rollback_gate = _rollback_gate(
        variant_validated_rows.get("baseline", []),
        overlap_fixture_summary=overlap_fixture_summary,
        current_label="baseline",
    )
    notes = [
        "experiment v1 当前仍是单标的时间序列 replay，对比的是同一批历史时点下不同权重方案，不代表已经通过全 universe promotion gate。",
        "这里的 champion / challenger 只用于研究优先级，不允许直接切换正式执行口径。",
    ]
    if int(overlap_fixture_summary.get("violation_count") or 0) > 0:
        notes.append("当前 experiment 样本存在 primary window overlap，结果只能当作敏感性比较，不能直接进入 promotion 讨论。")
    if baseline_row and champion_row and champion_row is not baseline_row:
        notes.append(
            f"当前样本里 `{champion_row.get('variant')}` 的 primary score 暂时高于 baseline，但还需要扩大样本并过外审，不能直接 promotion。"
        )
    if str(promotion_gate.get("status", "")) == "queue_for_next_stage":
        notes.append("当前最佳 challenger 已通过窄版 promotion gate，可进入下一阶段 validate / 外审，但仍不是直接切换正式执行口径。")
    elif str(promotion_gate.get("status", "")) == "stay_on_baseline":
        notes.append("当前 promotion gate 仍建议保留 baseline，challenger 继续留在研究观察区。")
    if str(rollback_gate.get("status", "")) == "watchlist":
        notes.append("当前 baseline 已进入 rollback watchlist，需要继续扩大 validated 样本并观察结构性 miss。")
    elif str(rollback_gate.get("status", "")) == "rollback_candidate":
        notes.append("当前 baseline 已进入 rollback 候选讨论，在更大验证前不应继续把它当成稳定 champion。")
    summary = {
        "symbol": symbol,
        "asset_type": asset_type,
        "start": str(start_stamp.date()),
        "end": str(end_stamp.date()),
        "asset_gap_days": max(int(asset_gap_days), 1),
        "batch_context": dict(batch_context or {}),
        "cohort_recipe": dict(cohort_recipe or {}),
        "sample_count": len(sample_indices),
        "variant_rows": variant_rows,
        "benchmark_fixture_summary": benchmark_fixture_summary,
        "lag_visibility_fixture_summary": _aggregate_lag_visibility_fixtures(
            [{"lag_visibility_fixture": {"status": "not_applicable", "strategy_candidate_ready_count": 0, "point_in_time_blocked_count": 0, "max_lag_days": 0}} for _ in sample_indices]
        ),
        "overlap_fixture_summary": overlap_fixture_summary,
        "promotion_gate": promotion_gate,
        "rollback_gate": rollback_gate,
        "baseline_variant": "baseline",
        "champion_variant": str(champion_row.get("variant", "")) if champion_row else "",
        "challenger_variant": str(challenger_row.get("variant", "")) if challenger_row else "",
        "notes": notes,
    }
    return summary


def generate_strategy_multi_symbol_experiment(
    symbols: Sequence[str],
    config: Mapping[str, Any],
    *,
    start: str = "",
    end: str = "",
    asset_gap_days: int = STRATEGY_V1_ASSET_GAP_DAYS,
    max_samples: int = 12,
    variants: Sequence[str] | None = None,
    batch_context: Mapping[str, Any] | None = None,
    cohort_recipe: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    normalized_symbols = _normalize_strategy_symbols(symbols)
    if not normalized_symbols:
        raise ValueError("至少需要一个 symbol 才能生成 strategy experiment。")
    variant_names = [str(item).strip() for item in (variants or ["baseline", "momentum_tilt", "defensive_tilt", "confirmation_tilt"]) if str(item).strip()]
    histories: Dict[str, Dict[str, Any]] = {}
    variant_rows: List[Dict[str, Any]] = []
    variant_validated_rows: Dict[str, List[Dict[str, Any]]] = {}
    benchmark_fixture_summary: Dict[str, Any] = {}
    overlap_fixture_summary: Dict[str, Any] = {}
    supply_summary: Dict[str, Any] = {}

    for symbol in normalized_symbols:
        asset_type = detect_asset_type(symbol, config)
        asset_history = _safe_normalize_history(fetch_asset_history(symbol, asset_type, dict(config)))
        benchmark_history = _safe_normalize_history(fetch_asset_history(STRATEGY_V1_BENCHMARK_SYMBOL, "cn_index", dict(config)))
        if asset_history.empty:
            raise ValueError(f"无法生成 strategy experiment，缺少 {symbol} 的完整日线。")
        start_stamp, end_stamp, sample_indices = _replay_sample_indices(
            asset_history,
            start=start,
            end=end,
            asset_gap_days=asset_gap_days,
            max_samples=max_samples,
        )
        histories[symbol] = {
            "asset_type": asset_type,
            "asset_history": asset_history,
            "benchmark_history": benchmark_history,
            "start_stamp": start_stamp,
            "end_stamp": end_stamp,
            "sample_indices": sample_indices,
        }

    for variant_name in variant_names:
        variant = dict(STRATEGY_V1_EXPERIMENT_VARIANTS.get(variant_name) or {})
        if not variant:
            raise ValueError(f"未知 experiment variant: {variant_name}")
        replay_rows: List[Dict[str, Any]] = []
        for symbol in normalized_symbols:
            history_payload = dict(histories.get(symbol) or {})
            asset_type = str(history_payload.get("asset_type", ""))
            asset_history = _safe_normalize_history(history_payload.get("asset_history"))
            benchmark_history = _safe_normalize_history(history_payload.get("benchmark_history"))
            for index in list(history_payload.get("sample_indices") or []):
                asset_slice = asset_history.iloc[: index + 1].copy()
                as_of = pd.Timestamp(asset_slice["date"].iloc[-1])
                benchmark_slice = benchmark_history[benchmark_history["date"] <= as_of].copy()
                replay_rows.append(
                    _build_replay_prediction(
                        symbol=symbol,
                        name=str(symbol),
                        asset_type=asset_type,
                        asset_history=asset_slice,
                        benchmark_history=benchmark_slice,
                        note=f"experiment variant={variant_name}",
                        weight_scheme=dict(variant.get("weight_scheme") or {}),
                        factor_version=f"{STRATEGY_V1_REPLAY_FACTOR_VERSION}:{variant_name}",
                        prediction_mode="historical_experiment_v1",
                        experiment_variant=variant_name,
                        asset_reentry_gap_days=asset_gap_days,
                    )
                )
        replay_rows, overlap_fixture_summary = _attach_overlap_fixtures(
            replay_rows,
            lambda symbol, _asset_type: _safe_normalize_history(dict(histories.get(symbol) or {}).get("asset_history")),
        )
        if not supply_summary:
            supply_summary = _cross_sectional_supply_summary(replay_rows)
        validated_rows, validation_summary = validate_strategy_rows(replay_rows, config)
        variant_validated_rows[variant_name] = list(validated_rows)
        _, attribution_summary = attribute_strategy_rows(validated_rows)
        if not benchmark_fixture_summary:
            benchmark_fixture_summary = _aggregate_benchmark_fixtures(replay_rows)
        out_of_sample_validation = dict(validation_summary.get("out_of_sample_validation") or {})
        cross_sectional_validation = dict(validation_summary.get("cross_sectional_validation") or {})
        dominant_label = ""
        dominant_count = 0
        if attribution_summary.get("label_rows"):
            dominant = list(attribution_summary.get("label_rows") or [])[0]
            dominant_label = str(dominant.get("label", ""))
            dominant_count = int(dominant.get("count", 0))
        variant_rows.append(
            {
                "variant": variant_name,
                "hypothesis": str(variant.get("hypothesis", "")),
                "sample_count": len(replay_rows),
                "validated_sample_count": int(validation_summary.get("validated_rows", 0)),
                "pending_sample_count": int(validation_summary.get("pending_rows", 0)),
                "out_of_sample_status": str(out_of_sample_validation.get("status", "")),
                "cross_sectional_status": str(cross_sectional_validation.get("status", "")),
                "cross_sectional_avg_rank_corr": _safe_float(cross_sectional_validation.get("avg_rank_corr")),
                "holdout_rows": int(dict(out_of_sample_validation.get("holdout_metrics") or {}).get("count", 0)),
                "holdout_avg_excess_return": _safe_float(dict(out_of_sample_validation.get("holdout_metrics") or {}).get("avg_excess_return")),
                "holdout_avg_cost_adjusted_directional_return": _safe_float(
                    dict(out_of_sample_validation.get("holdout_metrics") or {}).get("avg_cost_adjusted_directional_return")
                ),
                "hit_rate": _safe_float(validation_summary.get("hit_rate")),
                "avg_excess_return": _safe_float(validation_summary.get("avg_excess_return")),
                "avg_cost_adjusted_directional_return": _safe_float(validation_summary.get("avg_cost_adjusted_directional_return")),
                "avg_max_drawdown": _safe_float(validation_summary.get("avg_max_drawdown")),
                "dominant_attribution": dominant_label,
                "dominant_attribution_count": dominant_count,
                "primary_score": _experiment_primary_score(validation_summary),
            }
        )

    variant_rows.sort(key=lambda row: (float(row.get("primary_score", 0.0)), float(row.get("avg_excess_return", 0.0))), reverse=True)
    baseline_row = next((row for row in variant_rows if str(row.get("variant", "")) == "baseline"), None)
    champion_row = variant_rows[0] if variant_rows else None
    challenger_row = next((row for row in variant_rows if str(row.get("variant", "")) != "baseline"), None)
    promotion_gate = _promotion_gate(
        variant_rows,
        overlap_fixture_summary=overlap_fixture_summary,
        sample_count=sum(int(row.get("validated_sample_count", 0)) for row in variant_rows),
        require_cross_sectional=True,
    )
    rollback_gate = _rollback_gate(
        variant_validated_rows.get("baseline", []),
        overlap_fixture_summary=overlap_fixture_summary,
        current_label="baseline",
    )
    notes = [
        "experiment 已扩到多标的 replay 样本，promotion gate 会同时承认 out-of-sample 和 cross-sectional 状态。",
        "这仍然不是自动生产切换；只有通过下一阶段外审和更长窗口复核，才有资格讨论 production promotion。",
    ]
    if int(overlap_fixture_summary.get("violation_count") or 0) > 0:
        notes.append("当前 experiment 样本存在 primary window overlap，结果只能当作敏感性比较，不能直接进入 promotion 讨论。")
    if baseline_row and champion_row and champion_row is not baseline_row:
        notes.append(
            f"当前样本里 `{champion_row.get('variant')}` 的 primary score 暂时高于 baseline，但仍要结合 cross-sectional / out-of-sample 才能谈 promotion。"
        )
    if str(promotion_gate.get("status", "")) == "queue_for_next_stage":
        notes.append("当前最佳 challenger 已通过多标的窄版 promotion gate，可进入下一阶段更长窗口和外审。")
    elif str(promotion_gate.get("status", "")) == "stay_on_baseline":
        notes.append("当前 promotion gate 仍建议保留 baseline，challenger 继续留在研究观察区。")
    if str(rollback_gate.get("status", "")) == "watchlist":
        notes.append("当前 baseline 已进入 rollback watchlist，需要继续扩大 validated 样本并观察结构性 miss。")
    elif str(rollback_gate.get("status", "")) == "rollback_candidate":
        notes.append("当前 baseline 已进入 rollback 候选讨论，在更大验证前不应继续把它当成稳定 champion。")
    start_value = min(str(dict(histories.get(symbol) or {}).get("start_stamp").date()) for symbol in normalized_symbols if dict(histories.get(symbol) or {}).get("start_stamp")) if histories else start
    end_value = max(str(dict(histories.get(symbol) or {}).get("end_stamp").date()) for symbol in normalized_symbols if dict(histories.get(symbol) or {}).get("end_stamp")) if histories else end
    return {
        "symbol": ",".join(normalized_symbols),
        "symbols": normalized_symbols,
        "symbol_count": len(normalized_symbols),
        "scope": "multi_symbol_strategy_experiment_v1",
        "start": start or start_value,
        "end": end or end_value,
        "asset_gap_days": max(int(asset_gap_days), 1),
        "batch_context": dict(batch_context or {}),
        "cohort_recipe": dict(cohort_recipe or {}),
        "sample_count": sum(len(list(dict(histories.get(symbol) or {}).get("sample_indices") or [])) for symbol in normalized_symbols),
        "variant_rows": variant_rows,
        "cross_sectional_supply_summary": supply_summary,
        "benchmark_fixture_summary": benchmark_fixture_summary,
        "lag_visibility_fixture_summary": _aggregate_lag_visibility_fixtures(
            [{"lag_visibility_fixture": {"status": "not_applicable", "strategy_candidate_ready_count": 0, "point_in_time_blocked_count": 0, "max_lag_days": 0}} for _ in range(sum(len(list(dict(histories.get(symbol) or {}).get("sample_indices") or [])) for symbol in normalized_symbols))]
        ),
        "overlap_fixture_summary": overlap_fixture_summary,
        "promotion_gate": promotion_gate,
        "rollback_gate": rollback_gate,
        "baseline_variant": "baseline",
        "champion_variant": str(champion_row.get("variant", "")) if champion_row else "",
        "challenger_variant": str(challenger_row.get("variant", "")) if challenger_row else "",
        "notes": notes,
    }
