"""Shared helpers for Phase 4 portfolio risk workflows."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Mapping, Optional

import numpy as np
import pandas as pd

from src.processors.technical import normalize_ohlcv_frame
from src.storage.portfolio import PortfolioRepository
from src.utils.config import detect_asset_type, resolve_project_path
from src.utils.data import load_yaml
from src.utils.market import compute_history_metrics, fetch_asset_history, get_asset_context


REGION_BENCHMARKS: Dict[str, tuple[str, str]] = {
    "CN": ("510300", "cn_etf"),
    "US": ("SPY", "us"),
    "HK": ("HSTECH", "hk_index"),
}


@dataclass
class PortfolioRiskContext:
    """Portfolio market state used by risk and research commands."""

    status: Dict[str, Any]
    latest_prices: Dict[str, float]
    histories: Dict[str, pd.DataFrame]
    metrics: Dict[str, Dict[str, float]]
    returns_df: pd.DataFrame
    weights: Dict[str, float]
    benchmark_returns: pd.Series
    coverage_notes: List[str]


def build_portfolio_risk_context(
    config: Mapping[str, Any],
    repo: Optional[PortfolioRepository] = None,
    period: str = "3y",
) -> PortfolioRiskContext:
    """Load holdings, latest prices, aligned returns, and a blended benchmark."""
    repository = repo or PortfolioRepository()
    holdings = repository.list_holdings()
    latest_prices: Dict[str, float] = {}
    histories: Dict[str, pd.DataFrame] = {}
    metrics: Dict[str, Dict[str, float]] = {}
    return_series: Dict[str, pd.Series] = {}
    coverage_notes: List[str] = []

    for holding in holdings:
        symbol = str(holding["symbol"])
        asset_type = str(holding.get("asset_type") or detect_asset_type(symbol, config))
        try:
            history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config), period=period))
            history = trim_history_period(history, period)
            histories[symbol] = history
            metrics[symbol] = compute_history_metrics(history)
            latest_prices[symbol] = metrics[symbol]["last_close"]
            return_series[symbol] = history.set_index("date")["close"].pct_change().rename(symbol)
        except Exception as exc:
            latest_prices[symbol] = float(holding.get("cost_basis", 0.0))
            coverage_notes.append(f"{symbol} 历史数据不可用，风险计算已回退到成本价: {exc}")

    status = repository.build_status(latest_prices)
    available_rows = [row for row in status.get("holdings", []) if row["symbol"] in return_series]
    total_available_weight = sum(float(row["weight"]) for row in available_rows)
    weights = {
        row["symbol"]: (float(row["weight"]) / total_available_weight if total_available_weight else 0.0)
        for row in available_rows
    }

    if return_series:
        returns_df = pd.concat(return_series.values(), axis=1).sort_index().fillna(0.0)
    else:
        returns_df = pd.DataFrame()

    benchmark_returns = build_blended_benchmark(status, dict(config), period=period)
    if not weights:
        coverage_notes.append("当前没有可用于风险测算的持仓历史数据。")

    return PortfolioRiskContext(
        status=status,
        latest_prices=latest_prices,
        histories=histories,
        metrics=metrics,
        returns_df=returns_df,
        weights=weights,
        benchmark_returns=benchmark_returns,
        coverage_notes=coverage_notes,
    )


def build_blended_benchmark(
    status: Mapping[str, Any],
    config: Mapping[str, Any],
    period: str = "3y",
) -> pd.Series:
    """Blend region benchmarks according to current regional exposure."""
    components: List[pd.Series] = []
    weights: List[float] = []
    region_exposure = status.get("region_exposure", {}) or {}

    for region, exposure in region_exposure.items():
        benchmark = REGION_BENCHMARKS.get(str(region).upper())
        if not benchmark or exposure <= 0:
            continue
        symbol, asset_type = benchmark
        try:
            history = normalize_ohlcv_frame(fetch_asset_history(symbol, asset_type, dict(config), period=period))
            history = trim_history_period(history, period)
            series = history.set_index("date")["close"].pct_change().rename(region)
            components.append(series)
            weights.append(float(exposure))
        except Exception:
            continue

    if not components:
        return pd.Series(dtype=float, name="benchmark")

    matrix = pd.concat(components, axis=1).sort_index().fillna(0.0)
    weight_array = np.array(weights, dtype=float)
    weight_array = weight_array / weight_array.sum()
    benchmark_returns = (matrix * weight_array).sum(axis=1)
    benchmark_returns.name = "benchmark"
    return benchmark_returns


def load_stress_scenarios(config: Mapping[str, Any]) -> List[Dict[str, Any]]:
    """Load predefined stress scenarios from YAML."""
    path = config.get("stress_scenarios_path")
    if path:
        payload = load_yaml(resolve_project_path(path), default={"scenarios": []}) or {"scenarios": []}
    else:
        payload = load_yaml(
            resolve_project_path(config.get("stress_scenarios_file", "config/stress_scenarios.yaml")),
            default={"scenarios": []},
        ) or {"scenarios": []}
    scenarios = payload.get("scenarios", [])
    return [dict(item) for item in scenarios]


def resolve_stress_scenario(
    scenario: Mapping[str, Any],
    holdings: List[Dict[str, Any]],
    config: Mapping[str, Any],
) -> Dict[str, Any]:
    """Map thematic scenario shocks onto concrete holdings."""
    raw_shocks = {str(key).upper(): float(value) for key, value in dict(scenario.get("shocks", {})).items()}
    resolved_shocks: Dict[str, float] = {}
    mappings: Dict[str, str] = {}

    for holding in holdings:
        shock, source = _resolve_holding_shock(holding, raw_shocks, config)
        if shock is None or source is None:
            continue
        resolved_shocks[str(holding["symbol"])] = shock
        mappings[str(holding["symbol"])] = source

    return {
        "name": scenario.get("name", "unknown"),
        "description": scenario.get("description", ""),
        "shocks": resolved_shocks,
        "mappings": mappings,
        "raw_shocks": dict(scenario.get("shocks", {})),
    }


def find_stress_scenario(name: str, scenarios: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Find a scenario by exact name or substring match."""
    lowered = name.strip().lower()
    for item in scenarios:
        if str(item.get("name", "")).lower() == lowered:
            return item
    for item in scenarios:
        if lowered and lowered in str(item.get("name", "")).lower():
            return item
    return None


def trim_history_period(frame: pd.DataFrame, period: str) -> pd.DataFrame:
    """Trim a normalized price frame to a requested lookback period."""
    normalized = normalize_ohlcv_frame(frame)
    cutoff = _period_cutoff(period, normalized["date"].max())
    trimmed = normalized[normalized["date"] >= cutoff].reset_index(drop=True)
    return trimmed if len(trimmed) >= 30 else normalized


def _period_cutoff(period: str, end_date: pd.Timestamp) -> pd.Timestamp:
    token = period.strip().lower()
    number = ""
    unit = ""
    for char in token:
        if char.isdigit():
            number += char
        else:
            unit += char
    value = int(number or 3)
    if unit in {"y", "yr", "yrs", "year", "years"}:
        return end_date - pd.DateOffset(years=value)
    if unit in {"mo", "mos", "month", "months", "m"}:
        return end_date - pd.DateOffset(months=value)
    return end_date - pd.DateOffset(years=3)


def _resolve_holding_shock(
    holding: Mapping[str, Any],
    scenario_shocks: Mapping[str, float],
    config: Mapping[str, Any],
) -> tuple[Optional[float], Optional[str]]:
    symbol = str(holding["symbol"])
    asset_type = str(holding.get("asset_type") or detect_asset_type(symbol, config))
    context = get_asset_context(symbol, asset_type, dict(config))
    symbol_aliases = [symbol.upper(), context.source_symbol.upper()]
    for alias in symbol_aliases:
        if alias in scenario_shocks:
            return float(scenario_shocks[alias]), alias

    ordered_aliases: List[str] = []
    region = str(holding.get("region", "")).upper()
    sector = str(holding.get("sector", "")).upper()
    source_upper = context.source_symbol.upper()

    if asset_type == "cn_etf":
        ordered_aliases.extend(["CN_ETF", "A_SHARE"])
    if asset_type == "us":
        ordered_aliases.extend(["SPY", "US"])
    if asset_type in {"hk", "hk_index"}:
        ordered_aliases.extend(["HSTECH", "HK"])
    if region:
        ordered_aliases.append(region)
    if sector in {"科技".upper(), "TECH"}:
        ordered_aliases.append("TECH")
    if sector in {"黄金".upper(), "GOLD"}:
        ordered_aliases.extend(["GLD", "GOLD"])
    if sector in {"原油".upper(), "能源".upper(), "ENERGY"} or source_upper.startswith("SC"):
        ordered_aliases.append("OIL")

    for alias in ordered_aliases:
        if alias in scenario_shocks:
            return float(scenario_shocks[alias]), alias
    return None, None
