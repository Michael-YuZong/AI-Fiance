"""Portfolio construction, trade planning, and execution-cost helpers."""

from __future__ import annotations

from datetime import datetime
from math import sqrt
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

from src.processors.risk_support import build_blended_benchmark, build_portfolio_risk_context
from src.processors.horizon import build_trade_plan_horizon
from src.processors.technical import normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.storage.thesis import ThesisRepository
from src.utils.config import detect_asset_type
from src.utils.market import compute_history_metrics, fetch_asset_history, get_asset_context


_BROKER_FEE_RATES = {
    "cn_etf": 0.0003,
    "cn_stock": 0.0010,
    "cn_fund": 0.0050,
    "hk": 0.0025,
    "hk_index": 0.0020,
    "us": 0.0015,
    "futures": 0.0015,
}

_BASE_SLIPPAGE_BPS = {
    "cn_etf": 8.0,
    "cn_stock": 18.0,
    "hk": 20.0,
    "hk_index": 15.0,
    "us": 12.0,
    "futures": 15.0,
}


def _safe_float(value: Any, default: float = 0.0) -> float:
    try:
        number = float(value)
    except Exception:
        return default
    if np.isnan(number) or np.isinf(number):
        return default
    return number


def _history_as_of(history: pd.DataFrame) -> str:
    normalized = normalize_ohlcv_frame(history)
    if normalized.empty:
        return ""
    return str(pd.Timestamp(normalized["date"].iloc[-1]).date())


def _annualized_volatility(returns: pd.Series) -> float:
    clean = pd.to_numeric(returns, errors="coerce").dropna()
    if len(clean) < 2:
        return 0.0
    return float(clean.std() * np.sqrt(252))


def _portfolio_beta(returns_df: pd.DataFrame, weights: Mapping[str, float], benchmark_returns: pd.Series) -> float:
    columns = [column for column in returns_df.columns if column in weights]
    if not columns:
        return 0.0
    weight_array = np.array([float(weights.get(column, 0.0)) for column in columns], dtype=float)
    if not np.isfinite(weight_array).all() or float(weight_array.sum()) <= 0:
        return 0.0
    weight_array = weight_array / weight_array.sum()
    portfolio_returns = (returns_df[columns].fillna(0.0) * weight_array).sum(axis=1)
    aligned = pd.concat([portfolio_returns, benchmark_returns], axis=1, sort=False).dropna()
    if aligned.empty or float(aligned.iloc[:, 1].var()) == 0.0:
        return 0.0
    return float(np.cov(aligned.iloc[:, 0], aligned.iloc[:, 1])[0][1] / aligned.iloc[:, 1].var())


def estimate_execution_profile(
    *,
    asset_type: str,
    amount: float,
    price: float,
    metrics: Mapping[str, Any] | None = None,
    risk_limits: Mapping[str, Any] | None = None,
) -> Dict[str, Any]:
    """Estimate tradability and transaction cost for a proposed trade."""
    payload = dict(metrics or {})
    limits = dict(risk_limits or {})
    avg_turnover = max(_safe_float(payload.get("avg_turnover_20d"), 0.0), 0.0)
    quantity = amount / price if price else 0.0
    fee_rate = float(_BROKER_FEE_RATES.get(asset_type, 0.0010))

    if asset_type == "cn_fund":
        fee_cost = amount * fee_rate
        return {
            "execution_mode": "场外净值申赎",
            "tradability_label": "非即时成交",
            "avg_turnover_20d": avg_turnover,
            "participation_rate": None,
            "slippage_bps": 0.0,
            "estimated_slippage_cost": 0.0,
            "fee_rate": fee_rate,
            "estimated_fee_cost": fee_cost,
            "estimated_total_cost": fee_cost,
            "quantity": quantity,
            "liquidity_note": "场外基金按净值申赎，不适用场内滑点，更应关注费率和确认时滞。",
            "execution_note": "默认按申购/赎回处理，资金占用与确认时间通常慢于场内成交。",
            "max_participation_limit": float(limits.get("max_trade_participation", 0.05)),
        }

    base_bps = float(_BASE_SLIPPAGE_BPS.get(asset_type, 15.0))
    participation_rate = amount / avg_turnover if avg_turnover > 0 else None
    if participation_rate is None:
        slippage_bps = base_bps * 2.5
        tradability_label = "数据不足"
        liquidity_note = "没有拿到足够成交额样本，冲击成本只能按保守口径估算。"
    else:
        capped = min(max(participation_rate, 0.0), 0.25)
        slippage_bps = base_bps * (1.0 + 6.0 * sqrt(capped))
        if participation_rate <= 0.01:
            tradability_label = "顺畅"
        elif participation_rate <= 0.03:
            tradability_label = "可成交"
        elif participation_rate <= 0.08:
            tradability_label = "谨慎"
        else:
            tradability_label = "冲击偏高"
        liquidity_note = (
            f"这笔单约占近 20 日日均成交额的 `{participation_rate * 100:.2f}%`，"
            f"越接近日均成交额，冲击成本越容易抬升。"
        )
    fee_cost = amount * fee_rate
    slippage_cost = amount * slippage_bps / 10_000.0
    return {
        "execution_mode": "场内成交",
        "tradability_label": tradability_label,
        "avg_turnover_20d": avg_turnover,
        "participation_rate": participation_rate,
        "slippage_bps": float(slippage_bps),
        "estimated_slippage_cost": float(slippage_cost),
        "fee_rate": fee_rate,
        "estimated_fee_cost": float(fee_cost),
        "estimated_total_cost": float(fee_cost + slippage_cost),
        "quantity": float(quantity),
        "liquidity_note": liquidity_note,
        "execution_note": "这里只估场内成交冲击和显性费用，不含税务、隔夜跳空和极端行情冲击。",
        "max_participation_limit": float(limits.get("max_trade_participation", 0.05)),
    }


def build_trade_decision_snapshot(
    *,
    symbol: str,
    asset_type: str,
    config: Mapping[str, Any],
    history: pd.DataFrame,
    thesis: Mapping[str, Any] | None = None,
    period: str = "3y",
) -> Dict[str, Any]:
    context = get_asset_context(symbol, asset_type, dict(config))
    thesis_payload = dict(thesis or {})
    market_data_as_of = _history_as_of(history)
    notes = [
        f"行情快照只使用截止 `{market_data_as_of or '未知'}` 的 `{period}` 日线历史，不回看未来新闻或财报。",
        "组合预演默认把买入视为新增投入资金，把卖出视为释放现金，不自动假设同日换仓。",
    ]
    if thesis_payload:
        notes.append("本次动作会同步保留 thesis 快照，复盘时优先回看历史快照而不是当前回填。")
    else:
        notes.append("当前没有可同步保存的 thesis 快照，后续复盘会更依赖当时的信号和执行记录。")
    return {
        "recorded_at": datetime.now().isoformat(timespec="seconds"),
        "market_data_as_of": market_data_as_of,
        "market_data_source": context.source_symbol,
        "history_window": period,
        "thesis_snapshot_at": str(thesis_payload.get("updated_at") or thesis_payload.get("created_at") or ""),
        "notes": notes,
    }


def _project_holdings(
    status: Mapping[str, Any],
    *,
    symbol: str,
    name: str,
    asset_type: str,
    region: str,
    sector: str,
    action: str,
    amount: float,
) -> Dict[str, Any]:
    rows: List[Dict[str, Any]] = []
    current_value = 0.0
    existing_row: Dict[str, Any] | None = None
    for row in status.get("holdings", []) or []:
        cloned = dict(row)
        if str(row.get("symbol")) == symbol:
            existing_row = cloned
            current_value = _safe_float(cloned.get("market_value"), 0.0)
            cloned["market_value"] = max(current_value + amount, 0.0) if action == "buy" else max(current_value - amount, 0.0)
        rows.append(cloned)

    if existing_row is None and action == "buy":
        rows.append(
            {
                "symbol": symbol,
                "name": name,
                "asset_type": asset_type,
                "region": region,
                "sector": sector,
                "market_value": max(amount, 0.0),
                "weight": 0.0,
            }
        )

    rows = [row for row in rows if _safe_float(row.get("market_value"), 0.0) > 0.0]
    total_value = sum(_safe_float(row.get("market_value"), 0.0) for row in rows)
    region_exposure: Dict[str, float] = {}
    sector_exposure: Dict[str, float] = {}
    for row in rows:
        row["weight"] = _safe_float(row.get("market_value"), 0.0) / total_value if total_value else 0.0
        region_key = str(row.get("region", "UNKNOWN") or "UNKNOWN")
        sector_key = str(row.get("sector", "UNKNOWN") or "UNKNOWN")
        region_exposure[region_key] = region_exposure.get(region_key, 0.0) + float(row["weight"])
        sector_exposure[sector_key] = sector_exposure.get(sector_key, 0.0) + float(row["weight"])
    return {
        "holdings": rows,
        "total_value": total_value,
        "region_exposure": region_exposure,
        "sector_exposure": sector_exposure,
        "current_value": current_value,
    }


def _portfolio_risk_metrics(
    *,
    returns_df: pd.DataFrame,
    weights: Mapping[str, float],
    benchmark_returns: pd.Series,
) -> Dict[str, float]:
    columns = [column for column in returns_df.columns if column in weights and float(weights.get(column, 0.0)) > 0]
    if not columns:
        return {"annual_vol": 0.0, "beta": 0.0}
    normalized_weights = np.array([float(weights.get(column, 0.0)) for column in columns], dtype=float)
    normalized_weights = normalized_weights / normalized_weights.sum()
    portfolio_returns = (returns_df[columns].fillna(0.0) * normalized_weights).sum(axis=1)
    return {
        "annual_vol": _annualized_volatility(portfolio_returns),
        "beta": _portfolio_beta(returns_df[columns], dict(zip(columns, normalized_weights.tolist())), benchmark_returns),
    }


def build_trade_plan(
    *,
    action: str,
    symbol: str,
    price: float,
    amount: float,
    config: Mapping[str, Any],
    asset_type: str = "",
    repo: PortfolioRepository | None = None,
    thesis_repo: ThesisRepository | None = None,
    period: str = "3y",
) -> Dict[str, Any]:
    """Build a portfolio what-if plan for a proposed trade."""
    repository = repo or PortfolioRepository()
    thesis_repository = thesis_repo or ThesisRepository()
    asset_kind = asset_type or detect_asset_type(symbol, dict(config))
    asset_context = get_asset_context(symbol, asset_kind, dict(config))
    current_context = build_portfolio_risk_context(dict(config), repo=repository, period=period)
    thesis_record = dict(thesis_repository.get(symbol) or {})

    history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_kind, dict(config), period=period))
    metrics = compute_history_metrics(history)
    decision_snapshot = build_trade_decision_snapshot(
        symbol=symbol,
        asset_type=asset_kind,
        config=config,
        history=history,
        thesis=thesis_record,
        period=period,
    )
    risk_limits = dict(config.get("risk_limits", {}))
    execution = estimate_execution_profile(
        asset_type=asset_kind,
        amount=amount,
        price=price,
        metrics=metrics,
        risk_limits=risk_limits,
    )

    projected = _project_holdings(
        current_context.status,
        symbol=symbol,
        name=asset_context.name,
        asset_type=asset_kind,
        region=str(asset_context.metadata.get("region", "")),
        sector=str(asset_context.metadata.get("sector", "")),
        action=action,
        amount=amount,
    )

    current_weight = 0.0
    current_region = str(asset_context.metadata.get("region", "") or "UNKNOWN")
    current_sector = str(asset_context.metadata.get("sector", "") or "UNKNOWN")
    for row in current_context.status.get("holdings", []) or []:
        if str(row.get("symbol")) == symbol:
            current_weight = _safe_float(row.get("weight"), 0.0)
            current_region = str(row.get("region", current_region) or current_region)
            current_sector = str(row.get("sector", current_sector) or current_sector)
            break

    projected_weight = 0.0
    for row in projected["holdings"]:
        if str(row.get("symbol")) == symbol:
            projected_weight = _safe_float(row.get("weight"), 0.0)
            current_region = str(row.get("region", current_region) or current_region)
            current_sector = str(row.get("sector", current_sector) or current_sector)
            break

    single_position_max = float(risk_limits.get("single_position_max", 0.30))
    single_sector_max = float(risk_limits.get("single_sector_max", 0.40))
    single_region_max = float(risk_limits.get("single_region_max", 0.50))
    position_risk_budget = float(risk_limits.get("position_risk_budget", 0.035))

    sector_other_value = 0.0
    region_other_value = 0.0
    for row in current_context.status.get("holdings", []) or []:
        if str(row.get("symbol")) == symbol:
            continue
        market_value = _safe_float(row.get("market_value"), 0.0)
        if str(row.get("sector", "")) == current_sector:
            sector_other_value += market_value
        if str(row.get("region", "")) == current_region:
            region_other_value += market_value
    projected_total_value = max(_safe_float(projected["total_value"], 0.0), 0.0)
    allowed_by_sector = (
        max(single_sector_max * projected_total_value - sector_other_value, 0.0) / projected_total_value
        if projected_total_value
        else 0.0
    )
    allowed_by_region = (
        max(single_region_max * projected_total_value - region_other_value, 0.0) / projected_total_value
        if projected_total_value
        else 0.0
    )
    symbol_vol = max(_safe_float(metrics.get("volatility_20d"), 0.0), 0.0)
    allowed_by_risk_budget = single_position_max if symbol_vol <= 0 else min(single_position_max, position_risk_budget / max(symbol_vol, 0.08))
    suggested_max_weight = max(0.0, min(single_position_max, allowed_by_sector, allowed_by_region, allowed_by_risk_budget))

    returns_df = current_context.returns_df.copy()
    symbol_returns = history.set_index("date")["close"].pct_change().rename(symbol)
    if returns_df.empty:
        returns_df = pd.concat([symbol_returns], axis=1)
    else:
        returns_df = pd.concat([returns_df, symbol_returns], axis=1)
    returns_df = returns_df.sort_index().fillna(0.0)

    projected_weights = {
        str(row["symbol"]): float(row["weight"])
        for row in projected["holdings"]
        if str(row.get("symbol")) in returns_df.columns and float(row.get("weight", 0.0)) > 0
    }
    current_weights = {
        str(symbol_key): float(weight)
        for symbol_key, weight in current_context.weights.items()
        if str(symbol_key) in returns_df.columns and float(weight) > 0
    }
    projected_status = {
        "region_exposure": projected["region_exposure"],
    }
    projected_benchmark = build_blended_benchmark(projected_status, dict(config), period=period)
    current_risk = _portfolio_risk_metrics(
        returns_df=returns_df,
        weights=current_weights,
        benchmark_returns=current_context.benchmark_returns,
    )
    projected_risk = _portfolio_risk_metrics(
        returns_df=returns_df,
        weights=projected_weights,
        benchmark_returns=projected_benchmark,
    )

    alerts: List[str] = []
    if action == "sell" and amount > projected["current_value"] + 1e-9:
        alerts.append(f"计划卖出金额 `{amount:.2f}` 高于当前持仓市值 `{projected['current_value']:.2f}`，这更像超卖假设。")
    if projected_weight > suggested_max_weight + 1e-9:
        alerts.append(
            f"{symbol} 预演后权重约 `{projected_weight * 100:.1f}%`，高于当前可承受上限 `{suggested_max_weight * 100:.1f}%`。"
        )
    sector_weight = _safe_float(projected["sector_exposure"].get(current_sector), 0.0)
    region_weight = _safe_float(projected["region_exposure"].get(current_region), 0.0)
    if sector_weight > single_sector_max + 1e-9:
        alerts.append(f"行业 `{current_sector}` 暴露会升到 `{sector_weight * 100:.1f}%`，超过上限 `{single_sector_max * 100:.0f}%`。")
    if region_weight > single_region_max + 1e-9:
        alerts.append(f"地区 `{current_region}` 暴露会升到 `{region_weight * 100:.1f}%`，超过上限 `{single_region_max * 100:.0f}%`。")
    participation_limit = float(execution.get("max_participation_limit", 0.05))
    participation = execution.get("participation_rate")
    if participation is not None and float(participation) > participation_limit:
        alerts.append(
            f"这笔单约占日均成交额 `{float(participation) * 100:.2f}%`，高于保守参与率上限 `{participation_limit * 100:.1f}%`。"
        )

    if action == "buy":
        if projected_weight <= suggested_max_weight + 1e-9 and execution["tradability_label"] in {"顺畅", "可成交", "非即时成交"}:
            headline = (
                f"{symbol} 买入后仓位约 `{projected_weight * 100:.1f}%`，仍在组合和执行约束内，"
                "更像可以分批执行的首笔。"
            )
        else:
            headline = (
                f"{symbol} 这笔买入会把仓位推到 `{projected_weight * 100:.1f}%`，"
                f"而当前更合理的单票上限约是 `{suggested_max_weight * 100:.1f}%`。"
            )
    else:
        headline = (
            f"{symbol} 卖出后仓位约降到 `{projected_weight * 100:.1f}%`，"
            "更适合用来回收风险预算或缓解集中度。"
        )

    signal_snapshot = {
        "return_20d": metrics.get("return_20d"),
        "price_percentile_1y": metrics.get("price_percentile_1y"),
    }
    horizon = build_trade_plan_horizon(
        thesis=thesis_record,
        action=action,
        projected_weight=projected_weight,
        suggested_max_weight=suggested_max_weight,
        execution=execution,
        signal_snapshot=signal_snapshot,
    )
    decision_snapshot["horizon"] = dict(horizon)

    return {
        "action": action,
        "symbol": symbol,
        "name": asset_context.name,
        "asset_type": asset_kind,
        "price": price,
        "amount": amount,
        "current_weight": current_weight,
        "projected_weight": projected_weight,
        "suggested_max_weight": suggested_max_weight,
        "current_total_value": _safe_float(current_context.status.get("total_value"), 0.0),
        "projected_total_value": projected["total_value"],
        "current_sector": current_sector,
        "current_region": current_region,
        "projected_sector_weight": sector_weight,
        "projected_region_weight": region_weight,
        "current_risk": current_risk,
        "projected_risk": projected_risk,
        "execution": execution,
        "decision_snapshot": decision_snapshot,
        "horizon": horizon,
        "headline": headline,
        "alerts": alerts,
        "volatility_20d": symbol_vol,
        "avg_turnover_20d": _safe_float(metrics.get("avg_turnover_20d"), 0.0),
        "thesis_snapshot": thesis_record,
    }
