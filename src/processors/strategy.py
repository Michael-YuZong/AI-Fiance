"""Strategy v1 prediction-ledger helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, List, Mapping, Tuple

import pandas as pd

from src.processors.opportunity_engine import analyze_opportunity, build_market_context
from src.processors.provenance import history_as_of
from src.processors.technical import normalize_ohlcv_frame
from src.utils.config import detect_asset_type
from src.utils.market import fetch_asset_history


STRATEGY_V1_UNIVERSE = "a_share_liquid_stock_v1"
STRATEGY_V1_TARGET = "20d_excess_return_vs_csi800_rank"
STRATEGY_V1_BENCHMARK_SYMBOL = "000906.SH"
STRATEGY_V1_BENCHMARK_NAME = "中证800"
STRATEGY_V1_FACTOR_VERSION = "strategy_v1_seed_eight_dimension_2026-03-14"
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


def _eligibility_checks(
    *,
    symbol: str,
    analysis: Mapping[str, Any],
    benchmark_history: pd.DataFrame,
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
    if benchmark_normalized.empty:
        _add("benchmark_missing", "没有拿到中证800价格基准，主标签无法按合同定义。")

    return codes, reasons


def _downgrade_flags(analysis: Mapping[str, Any], benchmark_history: pd.DataFrame) -> List[str]:
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
    score_map = _dimension_score_map(dict(analysis.get("dimensions") or {}))
    seed_score = _seed_rank_score(score_map)
    confidence = _confidence_payload(seed_score)
    prediction_value = _prediction_value(seed_score)
    no_prediction_codes, no_prediction_reasons = _eligibility_checks(
        symbol=symbol,
        analysis=analysis,
        benchmark_history=benchmark_history,
    )
    as_of = str(dict(analysis.get("provenance") or {}).get("market_data_as_of", "")) or history_as_of(analysis.get("history"))
    created_at = datetime.now(UTC).isoformat(timespec="seconds")
    return {
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
        "factor_version": STRATEGY_V1_FACTOR_VERSION,
        "weight_scheme": dict(STRATEGY_V1_WEIGHT_SCHEME),
        "benchmark": {
            "symbol": STRATEGY_V1_BENCHMARK_SYMBOL,
            "name": STRATEGY_V1_BENCHMARK_NAME,
            "as_of": _safe_history_as_of(benchmark_history),
        },
        "regime": _regime_snapshot(analysis),
        "evidence_sources": _evidence_sources(analysis, benchmark_history),
        "downgrade_flags": _downgrade_flags(analysis, benchmark_history),
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


def generate_strategy_prediction(symbol: str, config: Mapping[str, Any], *, note: str = "") -> Dict[str, Any]:
    asset_type = detect_asset_type(symbol, config)
    context = build_market_context(dict(config), relevant_asset_types=["cn_stock", "cn_index", "cn_etf", "futures"])
    analysis = analyze_opportunity(symbol, asset_type, config, context=context, today_mode=False)
    try:
        benchmark_history = fetch_asset_history(STRATEGY_V1_BENCHMARK_SYMBOL, "cn_index", dict(config))
    except Exception:
        benchmark_history = pd.DataFrame()
    return build_strategy_prediction_from_analysis(analysis, benchmark_history=benchmark_history, note=note)
