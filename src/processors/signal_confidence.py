"""Historical analog confidence for current recommendation setups."""

from __future__ import annotations

from dataclasses import dataclass
from math import comb
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame


MIN_HISTORY_ROWS = 420
MIN_MATCH_SAMPLES = 12
DEFAULT_LOOKAHEAD_DAYS = 20
DEFAULT_NEIGHBORS = 20
DEFAULT_BOOTSTRAP_ITERATIONS = 1000
MIN_COVERAGE_MONTHS = 5
MIN_COVERAGE_DAYS = 120


@dataclass(frozen=True)
class AnalogMatch:
    index: int
    date: str
    distance: float
    return_5d: float
    return_20d: float
    mae_20d: float
    mfe_20d: float
    stop_hit: bool
    target_hit: bool


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError):
        return default
    if np.isnan(number) or np.isinf(number):
        return default
    return number


def _parse_pct(value: Any, *, default: float) -> float:
    text = str(value or "").strip()
    if not text:
        return default
    if text.endswith("%"):
        text = text[:-1]
    try:
        return abs(float(text)) / 100.0
    except ValueError:
        return default


def _rolling_price_percentile(close: pd.Series, window: int = 252) -> pd.Series:
    values = close.astype(float).to_numpy()
    result = np.full(len(values), np.nan, dtype=float)
    for idx in range(len(values)):
        start = max(0, idx - window + 1)
        segment = values[start : idx + 1]
        if len(segment) < min(window, 60):
            continue
        result[idx] = float(np.mean(segment <= values[idx]))
    return pd.Series(result, index=close.index, dtype=float)


def _build_feature_frame(
    history: pd.DataFrame,
    *,
    technical_config: Optional[Mapping[str, Any]] = None,
) -> pd.DataFrame:
    frame = normalize_ohlcv_frame(history)
    analyzer = TechnicalAnalyzer(frame)
    series = analyzer.indicator_series(dict(technical_config or {}))

    close = frame["close"].astype(float)
    high = frame["high"].astype(float)
    low = frame["low"].astype(float)
    volume = frame["volume"].astype(float).fillna(0.0)
    ma20 = close.rolling(20).mean()
    ma60 = close.rolling(60).mean()
    vol_ma20 = volume.rolling(20).mean()

    feature_frame = pd.DataFrame(
        {
            "date": pd.to_datetime(frame["date"]),
            "close": close,
            "high": high,
            "low": low,
            "ret_5d": close.pct_change(5),
            "ret_20d": close.pct_change(20),
            "price_pct_252": _rolling_price_percentile(close, window=252),
            "rsi": pd.to_numeric(series["rsi"], errors="coerce"),
            "adx": pd.to_numeric(series["adx"], errors="coerce"),
            "di_spread": (
                pd.to_numeric(series["plus_di"], errors="coerce") - pd.to_numeric(series["minus_di"], errors="coerce")
            )
            / 100.0,
            "macd_hist_pct": pd.to_numeric(series["macd_hist"], errors="coerce") / close.replace(0.0, np.nan),
            "ma_gap_20": close / ma20.replace(0.0, np.nan) - 1.0,
            "ma_gap_60": close / ma60.replace(0.0, np.nan) - 1.0,
            "vol_ratio_20": volume / vol_ma20.replace(0.0, np.nan),
            "natr": pd.to_numeric(series["atr"], errors="coerce") / close.replace(0.0, np.nan),
            "close_above_ma20": (close >= ma20).astype(float),
            "macd_positive": (pd.to_numeric(series["macd_hist"], errors="coerce") >= 0).astype(float),
            "di_positive": (
                pd.to_numeric(series["plus_di"], errors="coerce") >= pd.to_numeric(series["minus_di"], errors="coerce")
            ).astype(float),
        }
    )
    return feature_frame


def _future_outcome(
    frame: pd.DataFrame,
    index: int,
    *,
    lookahead_days: int,
    stop_loss_pct: float,
    target_pct: float,
) -> AnalogMatch | None:
    end = index + lookahead_days
    if end >= len(frame):
        return None

    entry = float(frame.iloc[index]["close"])
    future = frame.iloc[index + 1 : end + 1]
    if future.empty or not entry:
        return None

    ret_5_index = min(index + 5, len(frame) - 1)
    return_5d = float(frame.iloc[ret_5_index]["close"] / entry - 1) if ret_5_index > index else np.nan
    return_20d = float(frame.iloc[end]["close"] / entry - 1)
    mae_20d = float(future["low"].min() / entry - 1)
    mfe_20d = float(future["high"].max() / entry - 1)
    stop_hit = bool((future["low"] / entry - 1 <= -stop_loss_pct).any())
    target_hit = bool((future["high"] / entry - 1 >= target_pct).any())

    return AnalogMatch(
        index=index,
        date=str(pd.Timestamp(frame.iloc[index]["date"]).date()),
        distance=0.0,
        return_5d=return_5d,
        return_20d=return_20d,
        mae_20d=mae_20d,
        mfe_20d=mfe_20d,
        stop_hit=stop_hit,
        target_hit=target_hit,
    )


def _wilson_interval(successes: int, total: int, *, z: float = 1.96) -> tuple[float, float]:
    if total <= 0:
        return (0.0, 0.0)
    p_hat = successes / total
    denom = 1.0 + (z * z) / total
    center = (p_hat + (z * z) / (2.0 * total)) / denom
    margin = (
        z
        * np.sqrt((p_hat * (1.0 - p_hat) / total) + ((z * z) / (4.0 * total * total)))
        / denom
    )
    return (float(max(0.0, center - margin)), float(min(1.0, center + margin)))


def _bootstrap_interval(
    values: np.ndarray,
    *,
    statistic: str = "median",
    confidence: float = 0.95,
    iterations: int = DEFAULT_BOOTSTRAP_ITERATIONS,
    seed: int = 20260312,
) -> tuple[float, float]:
    clean = np.asarray(values, dtype=float)
    clean = clean[np.isfinite(clean)]
    if clean.size == 0:
        return (0.0, 0.0)
    if clean.size == 1:
        value = float(clean[0])
        return (value, value)

    rng = np.random.default_rng(seed + clean.size)
    indices = rng.integers(0, clean.size, size=(iterations, clean.size))
    samples = clean[indices]
    if statistic == "mean":
        stats = samples.mean(axis=1)
    else:
        stats = np.median(samples, axis=1)
    alpha = (1.0 - confidence) / 2.0
    low = float(np.quantile(stats, alpha))
    high = float(np.quantile(stats, 1.0 - alpha))
    return (low, high)


def _one_sided_binomial_pvalue(successes: int, total: int, *, baseline: float = 0.5) -> float:
    if total <= 0:
        return 1.0
    pvalue = 0.0
    for count in range(successes, total + 1):
        pvalue += comb(total, count) * (baseline**count) * ((1.0 - baseline) ** (total - count))
    return float(min(max(pvalue, 0.0), 1.0))


def _pick_non_overlapping_rows(
    ranked_rows: pd.DataFrame,
    *,
    min_spacing: int,
    max_count: int,
) -> pd.DataFrame:
    chosen_indices: list[int] = []
    chosen_rows: list[pd.Series] = []
    for idx, row in ranked_rows.iterrows():
        current_index = int(idx)
        if any(abs(current_index - previous) <= min_spacing for previous in chosen_indices):
            continue
        chosen_indices.append(current_index)
        chosen_rows.append(row)
        if len(chosen_rows) >= max_count:
            break
    if not chosen_rows:
        return ranked_rows.iloc[0:0].copy()
    return pd.DataFrame(chosen_rows)


def _score_label(score: int) -> str:
    if score >= 75:
        return "高"
    if score >= 60:
        return "中高"
    if score >= 45:
        return "中"
    return "低"


def _confidence_label(score: int) -> str:
    if score >= 75:
        return "高"
    if score >= 60:
        return "中高"
    if score >= 45:
        return "中"
    return "低"


def _confidence_summary(
    *,
    sample_count: int,
    coverage_months: int,
    win_rate_20d: float,
    win_rate_ci: tuple[float, float],
    median_return_20d: float,
    median_return_ci: tuple[float, float],
    avg_mae_20d: float,
    target_hit_rate: float,
    stop_hit_rate: float,
    quality_label: str,
    confidence_label: str,
) -> str:
    return (
        f"严格口径下保留 `{sample_count}` 个非重叠样本，覆盖约 `{coverage_months}` 个月；"
        f"20 日胜率约 `{win_rate_20d:.0%}`（95%区间 `{win_rate_ci[0]:.0%} ~ {win_rate_ci[1]:.0%}`），"
        f"20 日中位收益约 `{median_return_20d:+.1%}`（bootstrap 区间 `{median_return_ci[0]:+.1%} ~ {median_return_ci[1]:+.1%}`），"
        f"平均最大回撤约 `{avg_mae_20d:.1%}`。目标触达率 `{target_hit_rate:.0%}`，止损触发率 `{stop_hit_rate:.0%}`；"
        f"样本质量 `{quality_label}`，样本置信度 `{confidence_label}`。"
    )


def build_signal_confidence(
    history: pd.DataFrame,
    *,
    asset_type: str,
    technical_config: Optional[Mapping[str, Any]] = None,
    stop_loss_pct: Any = "-8%",
    target_pct: Any = 0.12,
    history_fallback: bool = False,
    lookahead_days: int = DEFAULT_LOOKAHEAD_DAYS,
    neighbors: int = DEFAULT_NEIGHBORS,
) -> Dict[str, Any]:
    if asset_type not in {"cn_stock", "hk", "us"}:
        return {
            "available": False,
            "reason": "当前只对个股提供同标的历史相似样本统计，ETF/基金暂不启用这层置信度。",
            "method": "same_symbol_daily_analog",
        }

    if history_fallback:
        return {
            "available": False,
            "reason": "当前用了历史降级快照，不能在低置信历史上继续推导相似样本统计。",
            "method": "same_symbol_daily_analog",
        }

    frame = normalize_ohlcv_frame(history)
    if len(frame) < MIN_HISTORY_ROWS:
        return {
            "available": False,
            "reason": f"历史样本不足：当前仅有 {len(frame)} 根日线，低于严格阈值 {MIN_HISTORY_ROWS}。",
            "method": "same_symbol_daily_analog",
        }

    feature_frame = _build_feature_frame(frame, technical_config=technical_config)
    required_cols = [
        "ret_5d",
        "ret_20d",
        "price_pct_252",
        "rsi",
        "adx",
        "di_spread",
        "macd_hist_pct",
        "ma_gap_20",
        "ma_gap_60",
        "vol_ratio_20",
        "natr",
    ]

    current = feature_frame.iloc[-1]
    if current[required_cols].isna().any():
        missing = [col for col in required_cols if pd.isna(current[col])]
        return {
            "available": False,
            "reason": "当前特征缺失，无法安全构建相似样本统计：" + "、".join(missing[:4]),
            "method": "same_symbol_daily_analog",
        }

    eligible = feature_frame.iloc[:-lookahead_days].copy()
    eligible = eligible.dropna(subset=required_cols)
    if eligible.empty:
        return {
            "available": False,
            "reason": "没有足够的历史窗口可用于前瞻回看。",
            "method": "same_symbol_daily_analog",
        }

    same_state = eligible[
        (eligible["macd_positive"] == current["macd_positive"])
        & (eligible["di_positive"] == current["di_positive"])
        & (eligible["close_above_ma20"] == current["close_above_ma20"])
    ].copy()
    if len(same_state) < MIN_MATCH_SAMPLES:
        same_state = eligible[
            (eligible["macd_positive"] == current["macd_positive"])
            & (eligible["di_positive"] == current["di_positive"])
        ].copy()

    if len(same_state) < MIN_MATCH_SAMPLES:
        return {
            "available": False,
            "reason": f"同方向历史样本不足：当前仅找到 {len(same_state)} 个可比日线场景，低于严格阈值 {MIN_MATCH_SAMPLES}。",
            "method": "same_symbol_daily_analog",
        }

    feature_cols = [
        "rsi",
        "adx",
        "di_spread",
        "macd_hist_pct",
        "ma_gap_20",
        "ma_gap_60",
        "ret_20d",
        "price_pct_252",
        "vol_ratio_20",
        "natr",
    ]
    medians = same_state[feature_cols].median()
    scales = (same_state[feature_cols].quantile(0.75) - same_state[feature_cols].quantile(0.25)).replace(0.0, np.nan)
    scales = scales.fillna(same_state[feature_cols].std(ddof=0).replace(0.0, np.nan)).fillna(1.0)

    normalized = ((same_state[feature_cols] - current[feature_cols]) / scales).astype(float)
    same_state["distance"] = ((normalized**2).mean(axis=1).astype(float)) ** 0.5
    same_state = same_state.sort_values("distance")

    selected = _pick_non_overlapping_rows(
        same_state,
        min_spacing=max(int(lookahead_days), 1),
        max_count=max(neighbors, MIN_MATCH_SAMPLES),
    )
    median_distance = float(selected["distance"].median()) if not selected.empty else np.inf
    if len(selected) < MIN_MATCH_SAMPLES or median_distance > 2.5:
        return {
            "available": False,
            "reason": (
                "相似样本距离过大或严格去重后的样本数量不足："
                f"当前仅保留 {len(selected)} 个非重叠样本，中位距离 {median_distance:.2f}。"
            ),
            "method": "same_symbol_daily_analog",
        }

    stop_pct = _parse_pct(stop_loss_pct, default=0.08)
    target_pct_float = float(target_pct) if not isinstance(target_pct, str) else _parse_pct(target_pct, default=0.12)
    target_pct_float = min(max(target_pct_float, 0.05), 0.30)

    matches: List[AnalogMatch] = []
    for idx, row in selected.iterrows():
        outcome = _future_outcome(
            frame,
            int(idx),
            lookahead_days=lookahead_days,
            stop_loss_pct=stop_pct,
            target_pct=target_pct_float,
        )
        if outcome is None:
            continue
        matches.append(
            AnalogMatch(
                index=outcome.index,
                date=outcome.date,
                distance=float(row["distance"]),
                return_5d=outcome.return_5d,
                return_20d=outcome.return_20d,
                mae_20d=outcome.mae_20d,
                mfe_20d=outcome.mfe_20d,
                stop_hit=outcome.stop_hit,
                target_hit=outcome.target_hit,
            )
        )

    if len(matches) < MIN_MATCH_SAMPLES:
        return {
            "available": False,
            "reason": f"可计算前瞻收益的相似样本不足：当前仅有 {len(matches)} 个有效样本。",
            "method": "same_symbol_daily_analog",
        }

    ret_5 = np.array([item.return_5d for item in matches], dtype=float)
    ret_20 = np.array([item.return_20d for item in matches], dtype=float)
    mae_20 = np.array([item.mae_20d for item in matches], dtype=float)
    mfe_20 = np.array([item.mfe_20d for item in matches], dtype=float)
    stop_hits = np.array([1.0 if item.stop_hit else 0.0 for item in matches], dtype=float)
    target_hits = np.array([1.0 if item.target_hit else 0.0 for item in matches], dtype=float)

    sample_count = len(matches)
    sample_dates_dt = pd.to_datetime([item.date for item in matches], errors="coerce").dropna()
    coverage_months = int(sample_dates_dt.to_period("M").nunique()) if not sample_dates_dt.empty else 0
    coverage_span_days = int((sample_dates_dt.max() - sample_dates_dt.min()).days) if len(sample_dates_dt) >= 2 else 0
    win_rate_5d = float(np.mean(ret_5 > 0))
    win_rate_20d = float(np.mean(ret_20 > 0))
    avg_return_5d = float(np.mean(ret_5))
    avg_return_20d = float(np.mean(ret_20))
    median_return_20d = float(np.median(ret_20))
    avg_mae_20d = float(np.mean(mae_20))
    avg_mfe_20d = float(np.mean(mfe_20))
    stop_hit_rate = float(np.mean(stop_hits))
    target_hit_rate = float(np.mean(target_hits))

    wins_20d = int(np.sum(ret_20 > 0))
    win_rate_ci = _wilson_interval(wins_20d, sample_count)
    avg_return_ci = _bootstrap_interval(ret_20, statistic="mean")
    median_return_ci = _bootstrap_interval(ret_20, statistic="median")
    sign_test_pvalue = _one_sided_binomial_pvalue(wins_20d, sample_count, baseline=0.5)

    quality_score = 0
    quality_score += 20 if sample_count >= 20 else 15 if sample_count >= 16 else 10
    quality_score += 15 if coverage_months >= 8 else 10 if coverage_months >= 6 else 5 if coverage_months >= MIN_COVERAGE_MONTHS else 0
    quality_score += 15 if coverage_span_days >= 240 else 10 if coverage_span_days >= 180 else 5 if coverage_span_days >= MIN_COVERAGE_DAYS else 0
    quality_score += 20 if median_distance <= 1.0 else 14 if median_distance <= 1.35 else 8 if median_distance <= 1.75 else 2
    win_ci_width = win_rate_ci[1] - win_rate_ci[0]
    quality_score += 15 if win_ci_width <= 0.22 else 10 if win_ci_width <= 0.32 else 5 if win_ci_width <= 0.42 else 0
    return_ci_width = median_return_ci[1] - median_return_ci[0]
    quality_score += 15 if return_ci_width <= 0.08 else 10 if return_ci_width <= 0.14 else 5 if return_ci_width <= 0.22 else 0
    quality_score = max(0, min(int(round(quality_score)), 100))
    quality_label = _score_label(quality_score)

    outcome_score = 0
    outcome_score += 30 if win_rate_20d >= 0.65 else 24 if win_rate_20d >= 0.58 else 16 if win_rate_20d >= 0.52 else 8 if win_rate_20d >= 0.47 else 0
    outcome_score += 20 if win_rate_ci[0] >= 0.50 else 10 if win_rate_20d > 0.50 else 0
    outcome_score += 20 if median_return_20d >= 0.08 else 15 if median_return_20d >= 0.04 else 8 if median_return_20d > 0 else 0
    outcome_score += 15 if median_return_ci[0] > 0 else 8 if median_return_20d > 0 else 0
    outcome_score += 15 if target_hit_rate >= stop_hit_rate + 0.10 else 8 if target_hit_rate >= stop_hit_rate else 0
    outcome_score = max(0, min(int(round(outcome_score)), 100))

    score = max(0, min(int(round(quality_score * 0.45 + outcome_score * 0.55)), 100))
    confidence_label = _confidence_label(score)

    quality_notes: List[str] = []
    if coverage_months < MIN_COVERAGE_MONTHS:
        quality_notes.append(f"样本时间覆盖只有 {coverage_months} 个月，时间分布偏窄。")
    if coverage_span_days < MIN_COVERAGE_DAYS:
        quality_notes.append(f"样本跨度只有 {coverage_span_days} 天，容易集中在单一行情段。")
    if win_rate_ci[0] < 0.50:
        quality_notes.append("20 日胜率的下沿还没站上 50%，统计优势不够硬。")
    if median_return_ci[0] <= 0:
        quality_notes.append("20 日收益区间下沿仍覆盖 0，历史结果更像弱优势而不是强优势。")
    if sign_test_pvalue > 0.10:
        quality_notes.append("胜率相对 50% 的统计强度一般，更多只能作为执行参考。")

    sample_dates = [item.date for item in matches[:5]]
    summary = _confidence_summary(
        sample_count=sample_count,
        coverage_months=coverage_months,
        win_rate_20d=win_rate_20d,
        win_rate_ci=win_rate_ci,
        median_return_20d=median_return_20d,
        median_return_ci=median_return_ci,
        avg_mae_20d=avg_mae_20d,
        target_hit_rate=target_hit_rate,
        stop_hit_rate=stop_hit_rate,
        quality_label=quality_label,
        confidence_label=confidence_label,
    )
    return {
        "available": True,
        "method": "same_symbol_daily_analog_strict",
        "scope": "同标的日线相似场景（严格非重叠样本）",
        "summary": summary,
        "reason": (
            "只使用当时可见的同标的日线量价/技术状态做相似样本，先按方向过滤，再按特征距离匹配，"
            "并强制去掉未来窗口重叠样本；不重建历史新闻、财报和完整基本面快照。"
            "因此这层更适合验证执行节奏和历史胜率，不等于完整策略回测。"
        ),
        "lookahead_days": lookahead_days,
        "sample_count": sample_count,
        "candidate_pool": int(len(same_state)),
        "non_overlapping_count": sample_count,
        "coverage_months": coverage_months,
        "coverage_span_days": coverage_span_days,
        "median_distance": median_distance,
        "sample_dates": sample_dates,
        "latest_sample_date": max(item.date for item in matches),
        "stop_loss_pct": stop_pct,
        "target_pct": target_pct_float,
        "win_rate_5d": win_rate_5d,
        "win_rate_20d": win_rate_20d,
        "avg_return_5d": avg_return_5d,
        "avg_return_20d": avg_return_20d,
        "avg_return_20d_ci_low": avg_return_ci[0],
        "avg_return_20d_ci_high": avg_return_ci[1],
        "median_return_20d": median_return_20d,
        "median_return_20d_ci_low": median_return_ci[0],
        "median_return_20d_ci_high": median_return_ci[1],
        "avg_mae_20d": avg_mae_20d,
        "avg_mfe_20d": avg_mfe_20d,
        "stop_hit_rate": stop_hit_rate,
        "target_hit_rate": target_hit_rate,
        "win_rate_20d_ci_low": win_rate_ci[0],
        "win_rate_20d_ci_high": win_rate_ci[1],
        "sign_test_pvalue": sign_test_pvalue,
        "sample_quality_score": quality_score,
        "sample_quality_label": quality_label,
        "quality_notes": quality_notes,
        "confidence_score": score,
        "confidence_label": confidence_label,
        "matches": [
            {
                "date": item.date,
                "distance": item.distance,
                "return_5d": item.return_5d,
                "return_20d": item.return_20d,
                "mae_20d": item.mae_20d,
                "mfe_20d": item.mfe_20d,
                "stop_hit": item.stop_hit,
                "target_hit": item.target_hit,
            }
            for item in matches[:5]
        ],
    }
