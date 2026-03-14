"""Strategy v1 prediction-ledger helpers."""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any, Dict, List, Mapping, Sequence, Tuple

import pandas as pd

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


def _technical_snapshot(history: pd.DataFrame) -> Dict[str, Any]:
    analyzer = TechnicalAnalyzer(history)
    return analyzer.generate_scorecard({})


def _replay_factor_engine(asset_history: pd.DataFrame, benchmark_history: pd.DataFrame) -> Dict[str, Any]:
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
    weight_scheme = {
        "medium_term_momentum": 0.28,
        "benchmark_relative": 0.30,
        "technical_confirmation": 0.20,
        "risk_efficiency": 0.17,
        "liquidity_profile": 0.05,
    }
    seed_score = round(
        sum(float(factor_scores[key]) * float(weight_scheme[key]) for key in factor_scores) / sum(weight_scheme.values()),
        2,
    )
    factor_snapshot = {
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
        "weight_scheme": weight_scheme,
        "factor_snapshot": factor_snapshot,
        "key_factors": key_factors,
    }


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


def _build_replay_prediction(
    *,
    symbol: str,
    name: str,
    asset_type: str,
    asset_history: pd.DataFrame,
    benchmark_history: pd.DataFrame,
    note: str = "",
) -> Dict[str, Any]:
    as_of = _safe_history_as_of(asset_history)
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
    )
    scorecard = _replay_factor_engine(asset_history, benchmark_history) if not no_prediction_codes else {
        "seed_score": 50.0,
        "weight_scheme": {},
        "factor_snapshot": {
            "price_momentum": {},
            "benchmark_relative": {},
            "technical": {},
            "liquidity": {},
            "risk": {},
        },
        "key_factors": [],
    }
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
        "prediction_mode": "historical_replay_v1",
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
        "factor_version": STRATEGY_V1_REPLAY_FACTOR_VERSION,
        "weight_scheme": dict(scorecard.get("weight_scheme") or {}),
        "benchmark": {
            "symbol": STRATEGY_V1_BENCHMARK_SYMBOL,
            "name": STRATEGY_V1_BENCHMARK_NAME,
            "as_of": _safe_history_as_of(benchmark_history),
        },
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
            "asset_reentry_gap_days": STRATEGY_V1_ASSET_GAP_DAYS,
            "overlap_policy": "single_symbol replay keeps primary samples non-overlapping by default",
        },
        "notes": notes,
    }


def generate_strategy_replay_predictions(
    symbol: str,
    config: Mapping[str, Any],
    *,
    start: str = "",
    end: str = "",
    note: str = "",
    asset_gap_days: int = STRATEGY_V1_ASSET_GAP_DAYS,
    max_samples: int = 12,
) -> Dict[str, Any]:
    asset_type = detect_asset_type(symbol, config)
    asset_history = _safe_normalize_history(fetch_asset_history(symbol, asset_type, dict(config)))
    benchmark_history = _safe_normalize_history(fetch_asset_history(STRATEGY_V1_BENCHMARK_SYMBOL, "cn_index", dict(config)))
    if asset_history.empty:
        raise ValueError(f"无法生成历史 replay，缺少 {symbol} 的完整日线。")
    start_stamp = pd.Timestamp(start) if start else pd.Timestamp(asset_history["date"].iloc[min(len(asset_history) - 1, 249)])
    end_stamp = pd.Timestamp(end) if end else pd.Timestamp(asset_history["date"].iloc[-1])
    rows: List[Dict[str, Any]] = []
    last_used_index = -10_000
    eligible_indices = [
        index
        for index, stamp in enumerate(asset_history["date"])
        if index >= 249 and pd.Timestamp(stamp) >= start_stamp and pd.Timestamp(stamp) <= end_stamp
    ]
    for index in eligible_indices:
        if index - last_used_index < max(int(asset_gap_days), 1):
            continue
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
        )
        rows.append(prediction)
        last_used_index = index
        if max_samples and len(rows) >= max(int(max_samples), 1):
            break
    return {
        "symbol": symbol,
        "asset_type": asset_type,
        "start": str(start_stamp.date()),
        "end": str(end_stamp.date()),
        "asset_gap_days": max(int(asset_gap_days), 1),
        "rows": rows,
        "notes": [
            "当前 replay 默认按单标的 non-overlap 主样本生成。",
            "这一步先建立历史样本，不代表已经完成全市场截面排序验证。",
        ],
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
        validation = _validation_snapshot(cloned, asset_history=asset_history, benchmark_history=benchmark_history)
        cloned["validation"] = validation
        updated_rows.append(cloned)
        if validation.get("validation_status") == "validated":
            evaluable.append(cloned)
        elif validation.get("validation_status") == "pending_future_window":
            pending += 1
        elif validation.get("validation_status") == "skipped_no_prediction":
            skipped += 1

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
        "bucket_rows": bucket_rows,
        "recent_rows": recent_rows,
        "notes": [
            "当前 validate 先做单标的时间序列口径，核心看超额收益方向、成本后方向收益和置信度分桶校准。",
            "这还不是全市场截面 rank 质量验证，后续要靠多标的/全 universe replay 才能补齐。",
        ],
    }
    return updated_rows, summary
