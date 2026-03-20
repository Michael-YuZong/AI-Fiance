"""Decision retrospective helpers for portfolio reviews."""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Mapping, MutableMapping, Optional, Sequence

import numpy as np
import pandas as pd

from src.processors.risk_support import REGION_BENCHMARKS
from src.processors.horizon import build_review_horizon
from src.processors.technical import TechnicalAnalyzer, normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository
from src.utils.config import detect_asset_type
from src.utils.market import compute_history_metrics, fetch_asset_history, get_asset_context


FORWARD_WINDOWS = (1, 3, 5, 20)


def _timestamp(value: Any) -> Optional[pd.Timestamp]:
    if value in (None, ""):
        return None
    try:
        stamp = pd.Timestamp(value)
    except Exception:
        return None
    if pd.isna(stamp):
        return None
    if stamp.tzinfo is not None:
        try:
            stamp = stamp.tz_convert("Asia/Shanghai").tz_localize(None)
        except TypeError:
            stamp = stamp.tz_localize(None)
    return stamp


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    if np.isnan(number) or np.isinf(number):
        return default
    return number


def _history_cache_key(symbol: str, asset_type: str) -> str:
    return f"{asset_type}:{symbol}"


def _resolve_entry_index(history: pd.DataFrame, trade_timestamp: Any) -> int:
    trade_day = (_timestamp(trade_timestamp) or pd.Timestamp.now()).normalize()
    normalized_dates = pd.to_datetime(history["date"], errors="coerce")
    for idx, value in enumerate(normalized_dates):
        if pd.isna(value):
            continue
        current = pd.Timestamp(value)
        if current.tzinfo is not None:
            current = current.tz_localize(None)
        if current.normalize() >= trade_day:
            return idx
    return max(len(history) - 1, 0)


def _rebuild_signal_snapshot(history: pd.DataFrame, end_index: int, technical_config: Mapping[str, Any]) -> Dict[str, Any]:
    sliced = normalize_ohlcv_frame(history.iloc[: end_index + 1])
    if sliced.empty:
        return {}
    technical = TechnicalAnalyzer(sliced).generate_scorecard(dict(technical_config))
    metrics = compute_history_metrics(sliced)
    dmi = dict(technical.get("dmi") or {})
    volume = dict(technical.get("volume") or {})
    volatility = dict(technical.get("volatility") or {})
    return {
        "return_20d": metrics.get("return_20d"),
        "price_percentile_1y": metrics.get("price_percentile_1y"),
        "ma_signal": dict(technical.get("ma_system") or {}).get("signal"),
        "macd_signal": dict(technical.get("macd") or {}).get("signal"),
        "rsi": dict(technical.get("rsi") or {}).get("RSI"),
        "adx": dmi.get("ADX"),
        "plus_di": dmi.get("DI+"),
        "minus_di": dmi.get("DI-"),
        "volume_signal": volume.get("signal"),
        "volume_structure": volume.get("structure"),
        "atr_signal": volatility.get("signal"),
    }


def _signal_alignment(signal_snapshot: Mapping[str, Any], action: str) -> str:
    ma_signal = str(signal_snapshot.get("ma_signal", "") or "").lower()
    macd_signal = str(signal_snapshot.get("macd_signal", "") or "").lower()
    plus_di = _safe_float(signal_snapshot.get("plus_di"), default=np.nan)
    minus_di = _safe_float(signal_snapshot.get("minus_di"), default=np.nan)
    trend_is_up = (
        ma_signal == "bullish"
        or macd_signal == "bullish"
        or (not np.isnan(plus_di) and not np.isnan(minus_di) and plus_di > minus_di)
    )
    trend_is_down = (
        ma_signal == "bearish"
        or macd_signal == "bearish"
        or (not np.isnan(plus_di) and not np.isnan(minus_di) and plus_di < minus_di)
    )

    if action == "buy":
        if trend_is_up and not trend_is_down:
            return "顺势买入"
        if trend_is_down and not trend_is_up:
            return "逆势买入"
        return "混合信号买入"

    if trend_is_down and not trend_is_up:
        return "顺势减仓"
    if trend_is_up and not trend_is_down:
        return "逆势减仓"
    return "混合信号减仓"


def _format_forward_map(path_returns: Mapping[int, Optional[float]]) -> Dict[str, str]:
    payload: Dict[str, str] = {}
    for days in FORWARD_WINDOWS:
        value = path_returns.get(days)
        payload[f"{days}d"] = "—" if value is None else f"{value * 100:+.2f}%"
    return payload


def _benchmark_spec(symbol: str, asset_type: str, config: Mapping[str, Any]) -> tuple[str, str]:
    context = get_asset_context(symbol, asset_type, dict(config))
    region = str(context.metadata.get("region", "")).upper()
    if region in REGION_BENCHMARKS:
        benchmark_symbol, benchmark_asset_type = REGION_BENCHMARKS[region]
        return benchmark_symbol, benchmark_asset_type
    if asset_type == "us":
        return REGION_BENCHMARKS["US"]
    if asset_type in {"hk", "hk_index"}:
        return REGION_BENCHMARKS["HK"]
    return REGION_BENCHMARKS["CN"]


def _forward_return(history: pd.DataFrame, trade_timestamp: Any, *, lookahead: int, action: str) -> Optional[float]:
    if history.empty:
        return None
    entry_index = _resolve_entry_index(history, trade_timestamp)
    if entry_index >= len(history):
        return None
    entry_price = _safe_float(history.iloc[entry_index]["close"])
    if entry_price <= 0:
        return None
    end_index = min(entry_index + max(int(lookahead), 1), len(history) - 1)
    if end_index <= entry_index:
        return None
    close_price = _safe_float(history.iloc[end_index]["close"], entry_price)
    raw_return = close_price / entry_price - 1 if entry_price else 0.0
    direction = 1.0 if action == "buy" else -1.0
    return raw_return * direction


def _setup_profile(
    signal_snapshot: Mapping[str, Any],
    thesis: Mapping[str, Any],
    *,
    action: str,
    signal_alignment: str,
) -> Dict[str, Any]:
    score = 0
    reasons: List[str] = []
    if "顺势" in signal_alignment:
        score += 35
        reasons.append("当时属于顺势动作")
    elif "混合" in signal_alignment:
        score += 20
        reasons.append("当时属于混合信号")
    else:
        score += 5
        reasons.append("当时更偏逆势动作")

    return_20d = _safe_float(signal_snapshot.get("return_20d"), default=np.nan)
    if not np.isnan(return_20d):
        if action == "buy":
            if return_20d > 0:
                score += 10
                reasons.append("近20日收益为正")
            elif return_20d > -0.05:
                score += 5
        else:
            if return_20d < 0:
                score += 10
                reasons.append("近20日收益与卖出方向一致")
            elif return_20d < 0.05:
                score += 5

    rsi = _safe_float(signal_snapshot.get("rsi"), default=np.nan)
    if not np.isnan(rsi):
        if 40 <= rsi <= 70:
            score += 10
            reasons.append("RSI 处在相对健康区间")
        elif 30 <= rsi <= 80:
            score += 5

    volume_structure = str(signal_snapshot.get("volume_structure", "") or "").strip()
    if volume_structure:
        score += 5
        reasons.append(f"量价结构记录为 {volume_structure}")

    if thesis:
        score += 10
        reasons.append("有同步 thesis 快照")

    score = max(0, min(score, 100))
    if score >= 55:
        bucket = "高把握"
    elif score >= 35:
        bucket = "中等把握"
    else:
        bucket = "低把握"
    return {"score": score, "bucket": bucket, "reasons": reasons[:4]}


def _attribution_summary(
    adjusted_return: Optional[float],
    benchmark_return: Optional[float],
) -> Dict[str, str]:
    if adjusted_return is None or benchmark_return is None:
        return {
            "label": "样本不足",
            "detail": "没有足够样本和基准窗口来区分这笔收益来自 alpha 还是 beta。",
        }

    excess_return = adjusted_return - benchmark_return
    if adjusted_return > 0 and excess_return >= 0.03:
        return {
            "label": "alpha兑现",
            "detail": "绝对收益为正，而且明显跑赢了同区基准，更多是标的/执行带来的超额结果。",
        }
    if adjusted_return > 0 and excess_return > -0.02:
        return {
            "label": "更多来自贝塔顺风",
            "detail": "结果为正，但相对同区基准没有明显超额，更像市场或板块顺风。",
        }
    if adjusted_return <= 0 and excess_return > 0:
        return {
            "label": "方向没错但执行/标的拖累",
            "detail": "大环境并不更差，但这笔动作没有把环境顺风转成自己的收益。",
        }
    return {
        "label": "方向与执行都偏弱",
        "detail": "绝对回报和相对收益都偏弱，需要一起复查方向、节奏和仓位。",
    }


def _summarize_review_proxy_contract(items: Sequence[Mapping[str, Any]]) -> Dict[str, Any]:
    market_flow: Dict[str, Any] = {}
    covered = 0
    total = 0
    confidence_labels: Counter[str] = Counter()
    limitation = ""
    downgrade_impact = ""
    for item in items:
        decision_snapshot = dict(item.get("decision_snapshot") or {})
        contract = dict(decision_snapshot.get("proxy_contract") or {})
        if not contract:
            continue
        current_market = dict(contract.get("market_flow") or {})
        if current_market and not market_flow:
            market_flow = current_market
        social = dict(contract.get("social_sentiment") or {})
        covered += int(social.get("covered") or 0)
        total += int(social.get("total") or 0)
        for label, count in dict(social.get("confidence_labels") or {}).items():
            try:
                confidence_labels[str(label)] += int(count)
            except Exception:
                continue
        if not limitation:
            limitation = str(current_market.get("limitation") or social.get("limitation") or "").strip()
        if not downgrade_impact:
            downgrade_impact = str(current_market.get("downgrade_impact") or social.get("downgrade_impact") or "").strip()
    if not market_flow and not total and not covered:
        return {}
    return {
        "market_flow": market_flow,
        "social_sentiment": {
            "covered": covered,
            "total": total,
            "confidence_labels": dict(sorted(confidence_labels.items())),
            "coverage_summary": f"{covered}/{total} 条历史决策保留了情绪代理快照" if total else "0/0 条历史决策保留了情绪代理快照",
            "limitation": limitation,
            "downgrade_impact": downgrade_impact,
        },
    }


def _evaluate_price_path(
    history: pd.DataFrame,
    *,
    entry_index: int,
    action: str,
    entry_price: float,
    lookahead: int,
    stop_pct: float,
    target_pct: float,
) -> Dict[str, Any]:
    if history.empty:
        return {
            "coverage_days": 0,
            "forward_returns": {days: None for days in FORWARD_WINDOWS},
            "adjusted_return": None,
            "mfe": None,
            "mae": None,
            "stop_level": None,
            "target_level": None,
            "stop_hit_day": None,
            "target_hit_day": None,
            "first_event": "无历史数据",
        }

    last_index = min(entry_index + max(int(lookahead), 1), len(history) - 1)
    window = history.iloc[entry_index : last_index + 1].reset_index(drop=True)
    forward_returns: Dict[int, Optional[float]] = {}
    direction = 1.0 if action == "buy" else -1.0
    for days in FORWARD_WINDOWS:
        offset = min(days, len(window) - 1)
        if offset <= 0 or offset >= len(window):
            forward_returns[days] = None
            continue
        close_price = _safe_float(window["close"].iloc[offset], entry_price)
        raw_return = close_price / entry_price - 1 if entry_price else 0.0
        forward_returns[days] = raw_return * direction

    highs = window["high"].astype(float)
    lows = window["low"].astype(float)
    stop_level = entry_price * (1 - stop_pct if action == "buy" else 1 + stop_pct)
    target_level = entry_price * (1 + target_pct if action == "buy" else 1 - target_pct)

    stop_hit_day: Optional[int] = None
    target_hit_day: Optional[int] = None
    for offset in range(1, len(window)):
        day_high = float(highs.iloc[offset])
        day_low = float(lows.iloc[offset])
        if action == "buy":
            if stop_hit_day is None and day_low <= stop_level:
                stop_hit_day = offset
            if target_hit_day is None and day_high >= target_level:
                target_hit_day = offset
        else:
            if stop_hit_day is None and day_high >= stop_level:
                stop_hit_day = offset
            if target_hit_day is None and day_low <= target_level:
                target_hit_day = offset

    if action == "buy":
        mfe = float(highs.max() / entry_price - 1) if entry_price else 0.0
        mae = float(lows.min() / entry_price - 1) if entry_price else 0.0
    else:
        mfe = float(1 - lows.min() / entry_price) if entry_price else 0.0
        mae = float(highs.max() / entry_price - 1) if entry_price else 0.0

    first_event = "窗口内未触发止损/目标"
    if stop_hit_day is not None and (target_hit_day is None or stop_hit_day <= target_hit_day):
        first_event = f"先触发止损（第 {stop_hit_day} 个交易日）"
    elif target_hit_day is not None:
        first_event = f"先触发目标（第 {target_hit_day} 个交易日）"

    coverage_days = max(len(window) - 1, 0)
    adjusted_return = forward_returns.get(min(lookahead, 20))
    if adjusted_return is None and coverage_days > 0:
        close_price = _safe_float(window["close"].iloc[-1], entry_price)
        raw_return = close_price / entry_price - 1 if entry_price else 0.0
        adjusted_return = raw_return * direction

    return {
        "coverage_days": coverage_days,
        "forward_returns": forward_returns,
        "adjusted_return": adjusted_return,
        "mfe": mfe,
        "mae": mae,
        "stop_level": stop_level,
        "target_level": target_level,
        "stop_hit_day": stop_hit_day,
        "target_hit_day": target_hit_day,
        "first_event": first_event,
    }


def _verdict(item: Mapping[str, Any]) -> Dict[str, str]:
    adjusted_return = item.get("adjusted_return")
    stop_hit_day = item.get("stop_hit_day")
    target_hit_day = item.get("target_hit_day")
    alignment = str(item.get("signal_alignment", ""))

    if stop_hit_day is not None and (target_hit_day is None or stop_hit_day <= target_hit_day):
        outcome = "结果偏差"
        detail = "价格路径先打到标准止损位，说明这次决策至少在执行窗口上不顺。"
    elif target_hit_day is not None:
        outcome = "结果兑现"
        detail = "价格路径先打到标准目标位，说明这次决策在窗口内兑现了预期。"
    elif adjusted_return is None:
        outcome = "样本不足"
        detail = "后验样本还不够长，当前只能看阶段性路径，不能轻易下最终结论。"
    elif adjusted_return >= 0.05:
        outcome = "结果偏正"
        detail = "虽然没有先触发目标，但窗口内收益仍明显站在有利方向。"
    elif adjusted_return <= -0.05:
        outcome = "结果偏弱"
        detail = "虽然没有先触发止损，但窗口内表现已经明显偏离原本方向。"
    else:
        outcome = "结果中性"
        detail = "这次决策没有明显兑现，也没有明显失效，更像是时间窗口不够理想。"

    if "顺势" in alignment and outcome in {"结果兑现", "结果偏正"}:
        summary = "顺势决策且后验结果匹配。"
    elif "逆势" in alignment and outcome in {"结果偏差", "结果偏弱"}:
        summary = "逆势决策且后验结果没有支持，应提高逆势门槛。"
    elif "顺势" in alignment and outcome in {"结果偏差", "结果偏弱"}:
        summary = "方向判断未必错，但执行窗口和赔率管理还需要更严格。"
    elif "逆势" in alignment and outcome in {"结果兑现", "结果偏正"}:
        summary = "结果站在你这边，但更多像逆势成功，不宜轻易把偶然当常态。"
    else:
        summary = "当时信号和后验结果并没有形成特别清晰的一致性。"

    return {
        "outcome": outcome,
        "detail": detail,
        "summary": summary,
    }


def _reason_lines(
    trade: Mapping[str, Any],
    thesis: Mapping[str, Any],
    signal_snapshot: Mapping[str, Any],
    verdict: Mapping[str, str],
) -> List[str]:
    lines: List[str] = []
    ma_signal = str(signal_snapshot.get("ma_signal", "") or "")
    macd_signal = str(signal_snapshot.get("macd_signal", "") or "")
    volume_structure = str(signal_snapshot.get("volume_structure", "") or "")
    return_20d = signal_snapshot.get("return_20d")
    if ma_signal or macd_signal:
        lines.append(f"当时均线/趋势信号偏 `{ma_signal or '未知'}`，MACD 偏 `{macd_signal or '未知'}`。")
    if volume_structure:
        lines.append(f"量价结构显示为 `{volume_structure}`，这会直接影响信号质量。")
    if return_20d is not None:
        lines.append(f"当时近 20 日收益约 `{_safe_float(return_20d) * 100:+.2f}%`，能反映你是在追趋势还是接回撤。")
    core = str(thesis.get("core_assumption", "") or thesis.get("core_hypothesis", "")).strip()
    if core:
        lines.append(f"当时可追溯到的核心 thesis 是：{core}")
    lines.append(verdict["summary"])
    return lines[:4]


def review_trade(
    trade: Mapping[str, Any],
    *,
    config: Mapping[str, Any],
    thesis_repo: ThesisRepository,
    history_cache: MutableMapping[str, pd.DataFrame],
    benchmark_cache: MutableMapping[str, pd.DataFrame],
    lookahead: int = 20,
    stop_pct: float = 0.08,
    target_pct: float = 0.15,
) -> Dict[str, Any]:
    symbol = str(trade.get("symbol", ""))
    asset_type = str(trade.get("asset_type") or detect_asset_type(symbol, config))
    cache_key = _history_cache_key(symbol, asset_type)
    if cache_key not in history_cache:
        history_cache[cache_key] = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config), period="3y"))
    history = history_cache[cache_key]
    if history.empty:
        raise ValueError(f"无法获取 {symbol} 的历史数据")

    entry_index = _resolve_entry_index(history, trade.get("timestamp"))
    entry_row = history.iloc[entry_index]
    entry_date = pd.Timestamp(entry_row["date"]).strftime("%Y-%m-%d")
    entry_price = _safe_float(trade.get("price"), _safe_float(entry_row["close"]))
    thesis_snapshot = dict(trade.get("thesis_snapshot") or {})
    thesis = thesis_snapshot or dict(thesis_repo.get(symbol) or {})
    signal_snapshot = dict(trade.get("signal_snapshot") or {})
    if not signal_snapshot or not signal_snapshot.get("ma_signal"):
        signal_snapshot = _rebuild_signal_snapshot(history, entry_index, dict(config.get("technical", {})))

    path = _evaluate_price_path(
        history,
        entry_index=entry_index,
        action=str(trade.get("action", "buy")),
        entry_price=entry_price,
        lookahead=lookahead,
        stop_pct=stop_pct,
        target_pct=target_pct,
    )
    signal_alignment = _signal_alignment(signal_snapshot, str(trade.get("action", "buy")))
    verdict = _verdict({**path, "signal_alignment": signal_alignment})
    reason_lines = _reason_lines(trade, thesis, signal_snapshot, verdict)
    benchmark_symbol, benchmark_asset_type = _benchmark_spec(symbol, asset_type, config)
    benchmark_key = _history_cache_key(benchmark_symbol, benchmark_asset_type)
    if benchmark_key not in benchmark_cache:
        benchmark_cache[benchmark_key] = normalize_ohlcv_frame(
            fetch_asset_history(benchmark_symbol, benchmark_asset_type, dict(config), period="3y")
        )
    benchmark_history = benchmark_cache[benchmark_key]
    benchmark_return = _forward_return(
        benchmark_history,
        trade.get("timestamp"),
        lookahead=lookahead,
        action=str(trade.get("action", "buy")),
    )
    adjusted_return = path.get("adjusted_return")
    excess_return = adjusted_return - benchmark_return if adjusted_return is not None and benchmark_return is not None else None
    attribution = _attribution_summary(adjusted_return, benchmark_return)
    setup_profile = _setup_profile(
        signal_snapshot,
        thesis,
        action=str(trade.get("action", "buy")),
        signal_alignment=signal_alignment,
    )
    decision_snapshot = dict(trade.get("decision_snapshot") or {})
    execution_snapshot = dict(trade.get("execution_snapshot") or {})
    horizon = build_review_horizon(
        thesis=thesis,
        signal_snapshot=signal_snapshot,
        action=str(trade.get("action", "buy")),
        signal_alignment=signal_alignment,
        decision_snapshot=decision_snapshot,
    )

    return {
        "symbol": symbol,
        "name": trade.get("name") or symbol,
        "asset_type": asset_type,
        "action": str(trade.get("action", "buy")),
        "basis": str(trade.get("basis", "unknown")),
        "timestamp": str(trade.get("timestamp", "")),
        "entry_date": entry_date,
        "entry_price": entry_price,
        "note": str(trade.get("note", "") or "").strip(),
        "signal_snapshot": signal_snapshot,
        "thesis": thesis,
        "thesis_is_historical": bool(thesis_snapshot),
        "signal_alignment": signal_alignment,
        "forward_returns": _format_forward_map(path["forward_returns"]),
        "adjusted_return": path["adjusted_return"],
        "benchmark_return": benchmark_return,
        "excess_return": excess_return,
        "benchmark_symbol": benchmark_symbol,
        "mfe": path["mfe"],
        "mae": path["mae"],
        "stop_level": path["stop_level"],
        "target_level": path["target_level"],
        "stop_hit_day": path["stop_hit_day"],
        "target_hit_day": path["target_hit_day"],
        "first_event": path["first_event"],
        "coverage_days": path["coverage_days"],
        "verdict": verdict,
        "reason_lines": reason_lines,
        "setup_profile": setup_profile,
        "attribution": attribution,
        "horizon": horizon,
        "decision_snapshot": decision_snapshot,
        "execution_snapshot": execution_snapshot,
    }


def build_monthly_decision_review(
    month: str,
    *,
    config: Mapping[str, Any],
    symbol: str = "",
    lookahead: int = 20,
    stop_pct: float = 0.08,
    target_pct: float = 0.15,
    repo: PortfolioRepository | None = None,
    thesis_repo: ThesisRepository | None = None,
) -> Dict[str, Any]:
    portfolio_repo = repo or PortfolioRepository()
    thesis_repository = thesis_repo or ThesisRepository()
    trades = [
        trade
        for trade in portfolio_repo.list_trades()
        if str(trade.get("timestamp", "")).startswith(month)
        and (not symbol or str(trade.get("symbol", "")) == symbol)
    ]
    history_cache: Dict[str, pd.DataFrame] = {}
    benchmark_cache: Dict[str, pd.DataFrame] = {}
    items = [
        review_trade(
            trade,
            config=config,
            thesis_repo=thesis_repository,
            history_cache=history_cache,
            benchmark_cache=benchmark_cache,
            lookahead=lookahead,
            stop_pct=stop_pct,
            target_pct=target_pct,
        )
        for trade in trades
    ]

    by_basis: Dict[str, Dict[str, float]] = defaultdict(
        lambda: {"count": 0, "wins": 0, "stop_hits": 0, "target_hits": 0, "avg_return": 0.0, "avg_excess": 0.0}
    )
    by_setup: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0, "wins": 0, "avg_return": 0.0, "avg_excess": 0.0})
    by_horizon: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0, "wins": 0, "avg_return": 0.0, "avg_excess": 0.0})
    attribution_counter: Dict[str, Dict[str, float]] = defaultdict(lambda: {"count": 0, "avg_return": 0.0, "avg_excess": 0.0})
    outcome_counter: Dict[str, int] = defaultdict(int)
    alignment_counter: Dict[str, int] = defaultdict(int)
    for item in items:
        basis_stats = by_basis[item["basis"]]
        basis_stats["count"] += 1
        adjusted = item.get("adjusted_return")
        excess = item.get("excess_return")
        if adjusted is not None:
            basis_stats["avg_return"] += float(adjusted)
            if float(adjusted) > 0:
                basis_stats["wins"] += 1
        if excess is not None:
            basis_stats["avg_excess"] += float(excess)
        if item.get("stop_hit_day") is not None:
            basis_stats["stop_hits"] += 1
        if item.get("target_hit_day") is not None:
            basis_stats["target_hits"] += 1
        outcome_counter[item["verdict"]["outcome"]] += 1
        alignment_counter[item["signal_alignment"]] += 1

        setup_bucket = str(dict(item.get("setup_profile") or {}).get("bucket", "未知"))
        setup_stats = by_setup[setup_bucket]
        setup_stats["count"] += 1
        if adjusted is not None:
            setup_stats["avg_return"] += float(adjusted)
            if float(adjusted) > 0:
                setup_stats["wins"] += 1
        if excess is not None:
            setup_stats["avg_excess"] += float(excess)

        horizon_label = str(dict(item.get("horizon") or {}).get("label", "观察期"))
        horizon_stats = by_horizon[horizon_label]
        horizon_stats["count"] += 1
        if adjusted is not None:
            horizon_stats["avg_return"] += float(adjusted)
            if float(adjusted) > 0:
                horizon_stats["wins"] += 1
        if excess is not None:
            horizon_stats["avg_excess"] += float(excess)

        attribution_label = str(dict(item.get("attribution") or {}).get("label", "未知"))
        attribution_stats = attribution_counter[attribution_label]
        attribution_stats["count"] += 1
        if adjusted is not None:
            attribution_stats["avg_return"] += float(adjusted)
        if excess is not None:
            attribution_stats["avg_excess"] += float(excess)

    basis_rows: List[List[str]] = []
    for basis, stats in sorted(by_basis.items()):
        count = int(stats["count"] or 0)
        avg_return = (stats["avg_return"] / count) if count else 0.0
        avg_excess = (stats["avg_excess"] / count) if count else 0.0
        win_rate = stats["wins"] / count if count else 0.0
        stop_rate = stats["stop_hits"] / count if count else 0.0
        target_rate = stats["target_hits"] / count if count else 0.0
        basis_rows.append(
            [
                basis,
                str(count),
                f"{avg_return * 100:+.2f}%",
                f"{avg_excess * 100:+.2f}%",
                f"{win_rate * 100:.1f}%",
                f"{stop_rate * 100:.1f}%",
                f"{target_rate * 100:.1f}%",
            ]
        )

    setup_rows: List[List[str]] = []
    setup_rank = {"高把握": 0, "中等把握": 1, "低把握": 2}
    for bucket, stats in sorted(by_setup.items(), key=lambda item: setup_rank.get(item[0], 99)):
        count = int(stats["count"] or 0)
        avg_return = (stats["avg_return"] / count) if count else 0.0
        avg_excess = (stats["avg_excess"] / count) if count else 0.0
        win_rate = stats["wins"] / count if count else 0.0
        setup_rows.append(
            [
                bucket,
                str(count),
                f"{avg_return * 100:+.2f}%",
                f"{avg_excess * 100:+.2f}%",
                f"{win_rate * 100:.1f}%",
            ]
        )

    attribution_rows: List[List[str]] = []
    for label, stats in sorted(attribution_counter.items(), key=lambda item: int(item[1]["count"]), reverse=True):
        count = int(stats["count"] or 0)
        avg_return = (stats["avg_return"] / count) if count else 0.0
        avg_excess = (stats["avg_excess"] / count) if count else 0.0
        attribution_rows.append(
            [
                label,
                str(count),
                f"{avg_return * 100:+.2f}%",
                f"{avg_excess * 100:+.2f}%",
            ]
        )

    horizon_rows: List[List[str]] = []
    horizon_rank = {
        "观察期": 0,
        "短线交易（3-10日）": 1,
        "波段跟踪（2-6周）": 2,
        "中线配置（1-3月）": 3,
        "长线配置（6-12月）": 4,
    }
    for label, stats in sorted(by_horizon.items(), key=lambda item: horizon_rank.get(item[0], 99)):
        count = int(stats["count"] or 0)
        avg_return = (stats["avg_return"] / count) if count else 0.0
        avg_excess = (stats["avg_excess"] / count) if count else 0.0
        win_rate = stats["wins"] / count if count else 0.0
        horizon_rows.append(
            [
                label,
                str(count),
                f"{avg_return * 100:+.2f}%",
                f"{avg_excess * 100:+.2f}%",
                f"{win_rate * 100:.1f}%",
            ]
        )

    summary_lines: List[str] = []
    if items:
        summary_lines.append(f"本次共回看 `{len(items)}` 笔决策，标准观察窗口为 `{lookahead}` 个交易日。")
        if outcome_counter:
            major_outcome = max(outcome_counter.items(), key=lambda item: item[1])[0]
            summary_lines.append(f"最常见的后验结果是 `{major_outcome}`。")
        if alignment_counter:
            major_alignment = max(alignment_counter.items(), key=lambda item: item[1])[0]
            summary_lines.append(f"信号一致性里占比最高的是 `{major_alignment}`。")
        if attribution_rows:
            summary_lines.append(f"最常见的收益归因是 `{attribution_rows[0][0]}`。")
        if by_horizon:
            most_common_horizon = max(by_horizon.items(), key=lambda item: int(item[1]["count"] or 0))[0]
            summary_lines.append(f"本月最常见的执行周期是 `{most_common_horizon}`。")
        if by_setup.get("高把握") and by_setup.get("低把握"):
            high_count = int(by_setup["高把握"]["count"] or 0)
            low_count = int(by_setup["低把握"]["count"] or 0)
            if high_count and low_count:
                high_avg = by_setup["高把握"]["avg_return"] / high_count
                low_avg = by_setup["低把握"]["avg_return"] / low_count
                summary_lines.append(
                    f"`高把握` setup 的平均结果约 `{high_avg * 100:+.2f}%`，`低把握` 约 `{low_avg * 100:+.2f}%`。"
                )
        if any(not item["thesis_is_historical"] and item["thesis"] for item in items):
            summary_lines.append("部分旧交易没有历史 thesis 快照，报告已回退到当前 thesis，仅可作辅助参考。")
    else:
        summary_lines.append("该月份没有可回溯的交易记录。")

    return {
        "title": "决策回溯",
        "month": month,
        "symbol": symbol,
        "generated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "lookahead": lookahead,
        "stop_pct": stop_pct,
        "target_pct": target_pct,
        "summary_lines": summary_lines,
        "basis_rows": basis_rows,
        "setup_rows": setup_rows,
        "horizon_rows": horizon_rows,
        "attribution_rows": attribution_rows,
        "proxy_contract": _summarize_review_proxy_contract(items),
        "items": items,
    }
