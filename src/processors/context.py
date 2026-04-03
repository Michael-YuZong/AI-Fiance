"""Macro and market context snapshots used by Phase 2 commands."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Mapping, Optional, Sequence, Tuple

import pandas as pd

from src.collectors.macro_cn import ChinaMacroCollector
from src.utils.market import market_regime_proxy


def _sort_latest_first(frame: pd.DataFrame) -> pd.DataFrame:
    order_candidates = ("month", "MONTH", "date", "日期", "月份")
    for column in order_candidates:
        if column not in frame.columns:
            continue
        ordering = pd.to_numeric(frame[column].astype(str).str.replace(r"\D", "", regex=True), errors="coerce")
        if ordering.notna().any():
            ranked = frame.assign(_order=ordering)
            ranked = ranked.sort_values("_order", ascending=False, kind="stable")
            return ranked.drop(columns="_order")
    return frame


def _first_existing_column(frame: pd.DataFrame, candidates: Sequence[str]) -> str:
    for column in candidates:
        if column in frame.columns:
            return column
    available = ", ".join(map(str, frame.columns.tolist()))
    expected = ", ".join(candidates)
    raise KeyError(f"Expected one of [{expected}], got columns [{available}]")


def _series_pair(frame: pd.DataFrame, candidates: Sequence[str]) -> Tuple[float, float]:
    column = _first_existing_column(frame, candidates)
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        raise ValueError(f"No valid values for column: {column}")
    current = float(series.iloc[0])
    previous = float(series.iloc[1]) if len(series) > 1 else current
    return current, previous


def _series_pair_or_default(frame: pd.DataFrame, candidates: Sequence[str], default: Tuple[float, float]) -> Tuple[float, float]:
    try:
        return _series_pair(frame, candidates)
    except Exception:
        return default


def _three_month_average(frame: pd.DataFrame, candidates: Sequence[str]) -> Tuple[float, float]:
    column = _first_existing_column(frame, candidates)
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        raise ValueError(f"No valid values for column: {column}")
    recent = float(series.iloc[:3].mean())
    previous = float(series.iloc[3:6].mean()) if len(series) >= 6 else float(series.iloc[1:4].mean()) if len(series) >= 4 else recent
    return recent, previous


def _trend(current: float, previous: float, *, tolerance: float = 0.05) -> str:
    if current > previous + tolerance:
        return "rising"
    if current < previous - tolerance:
        return "falling"
    return "stable"


def _pct_trend(current: float, previous: float, *, tolerance: float = 0.08, absolute_floor: float = 1e-6) -> str:
    base = max(abs(previous), absolute_floor)
    diff_ratio = (current - previous) / base
    if diff_ratio > tolerance:
        return "rising"
    if diff_ratio < -tolerance:
        return "falling"
    return "stable"


def _fmt_sf_trillion(value: float) -> str:
    # Tushare/AK 社融增量常见口径是亿元。
    return f"{value / 10000:.2f} 万亿元"


def _credit_impulse_label(
    pmi: float,
    pmi_trend: str,
    m1_m2_spread: float,
    spread_trend: str,
    sf_trend: str,
) -> str:
    positive = 0
    negative = 0
    if pmi >= 50 and pmi_trend == "rising":
        positive += 1
    elif pmi < 50 and pmi_trend == "falling":
        negative += 1
    if m1_m2_spread > -1.0 or spread_trend == "rising":
        positive += 1
    if m1_m2_spread < -4.0 and spread_trend == "falling":
        negative += 1
    if sf_trend == "rising":
        positive += 1
    elif sf_trend == "falling":
        negative += 1

    if positive >= 2 and negative == 0:
        return "expanding"
    if negative >= 2 and positive == 0:
        return "contracting"
    return "stable"


def load_china_macro_snapshot(config: Dict[str, Any]) -> Dict[str, float]:
    collector = ChinaMacroCollector(config)

    pmi_frame = _sort_latest_first(collector.get_pmi())
    pmi, pmi_prev = _series_pair(pmi_frame, ("制造业-指数", "PMI010000", "PMI", "制造业PMI"))
    pmi_production, pmi_production_prev = _series_pair_or_default(pmi_frame, ("生产指数", "PMI010400"), (pmi, pmi_prev))
    pmi_new_orders, pmi_new_orders_prev = _series_pair_or_default(pmi_frame, ("新订单指数", "PMI010500"), (pmi, pmi_prev))
    pmi_export_orders, pmi_export_orders_prev = _series_pair_or_default(pmi_frame, ("新出口订单指数", "PMI010900"), (pmi, pmi_prev))
    pmi_raw_inventory, pmi_raw_inventory_prev = _series_pair_or_default(pmi_frame, ("原材料库存指数", "PMI010700"), (50.0, 50.0))
    pmi_finished_inventory, pmi_finished_inventory_prev = _series_pair_or_default(pmi_frame, ("产成品库存指数", "PMI011400"), (50.0, 50.0))

    cpi_frame = _sort_latest_first(collector.get_cpi())
    cpi, cpi_prev = _series_pair(cpi_frame, ("今值", "nt_yoy", "CPI同比", "全国同比"))

    ppi_frame = _sort_latest_first(collector.get_ppi())
    ppi, ppi_prev = _series_pair_or_default(ppi_frame, ("ppi_yoy", "PPI同比"), (0.0, 0.0))

    money_frame = _sort_latest_first(collector.get_money_supply())
    m1_yoy, m1_prev = _series_pair_or_default(money_frame, ("m1_yoy", "M1同比"), (0.0, 0.0))
    m2_yoy, m2_prev = _series_pair_or_default(money_frame, ("m2_yoy", "M2同比"), (0.0, 0.0))
    m1_m2_spread = m1_yoy - m2_yoy
    m1_m2_spread_prev = m1_prev - m2_prev

    sf_frame = _sort_latest_first(collector.get_social_financing())
    sf_month, sf_month_prev = _series_pair_or_default(sf_frame, ("社会融资规模增量", "社融增量", "inc_month", "increment"), (0.0, 0.0))
    try:
        sf_3m_avg, sf_prev_3m_avg = _three_month_average(sf_frame, ("社会融资规模增量", "社融增量", "inc_month", "increment"))
    except Exception:
        sf_3m_avg, sf_prev_3m_avg = sf_month, sf_month_prev

    lpr_frame = _sort_latest_first(collector.get_lpr())
    lpr, lpr_prev = _series_pair(lpr_frame, ("LPR1Y", "1y"))

    pmi_trend = _trend(pmi, pmi_prev, tolerance=0.15)
    cpi_trend = _trend(cpi, cpi_prev, tolerance=0.1)
    ppi_trend = _trend(ppi, ppi_prev, tolerance=0.2)
    m1_m2_spread_trend = _trend(m1_m2_spread, m1_m2_spread_prev, tolerance=0.3)
    social_financing_trend = _pct_trend(sf_3m_avg, sf_prev_3m_avg, tolerance=0.08)
    credit_impulse = _credit_impulse_label(
        pmi=pmi,
        pmi_trend=pmi_trend,
        m1_m2_spread=m1_m2_spread,
        spread_trend=m1_m2_spread_trend,
        sf_trend=social_financing_trend,
    )

    if pmi >= 50 and pmi_new_orders >= 50 and pmi_new_orders_prev <= pmi_new_orders:
        demand_state = "improving"
    elif pmi < 50 and pmi_new_orders < 50 and pmi_new_orders_prev >= pmi_new_orders:
        demand_state = "weakening"
    else:
        demand_state = "stable"

    if pmi_finished_inventory > 50 and pmi_new_orders < 50:
        inventory_state = "destocking_pressure"
    elif pmi_new_orders > 50 and pmi_raw_inventory < 50:
        inventory_state = "restocking_tailwind"
    else:
        inventory_state = "balanced"

    if ppi >= 0 or (ppi > -1.0 and ppi_trend == "rising"):
        price_state = "reflation"
    elif ppi < 0 and ppi_trend == "falling":
        price_state = "disinflation"
    else:
        price_state = "stable"

    return {
        "pmi": pmi,
        "pmi_prev": pmi_prev,
        "pmi_trend": pmi_trend,
        "pmi_production": pmi_production,
        "pmi_production_prev": pmi_production_prev,
        "pmi_new_orders": pmi_new_orders,
        "pmi_new_orders_prev": pmi_new_orders_prev,
        "pmi_export_orders": pmi_export_orders,
        "pmi_export_orders_prev": pmi_export_orders_prev,
        "pmi_raw_inventory": pmi_raw_inventory,
        "pmi_raw_inventory_prev": pmi_raw_inventory_prev,
        "pmi_finished_inventory": pmi_finished_inventory,
        "pmi_finished_inventory_prev": pmi_finished_inventory_prev,
        "demand_state": demand_state,
        "inventory_state": inventory_state,
        "cpi_monthly": cpi,
        "cpi_prev": cpi_prev,
        "cpi_trend": cpi_trend,
        "ppi_yoy": ppi,
        "ppi_prev": ppi_prev,
        "ppi_trend": ppi_trend,
        "price_state": price_state,
        "m1_yoy": m1_yoy,
        "m1_prev": m1_prev,
        "m2_yoy": m2_yoy,
        "m2_prev": m2_prev,
        "m1_m2_spread": m1_m2_spread,
        "m1_m2_spread_prev": m1_m2_spread_prev,
        "m1_m2_spread_trend": m1_m2_spread_trend,
        "social_financing_month": sf_month,
        "social_financing_prev": sf_month_prev,
        "social_financing_3m_avg": sf_3m_avg,
        "social_financing_prev_3m_avg": sf_prev_3m_avg,
        "social_financing_trend": social_financing_trend,
        "social_financing_3m_avg_text": _fmt_sf_trillion(sf_3m_avg),
        "credit_impulse": credit_impulse,
        "lpr_1y": lpr,
        "lpr_prev": lpr_prev,
    }


def global_proxy_runtime_enabled(config: Mapping[str, Any] | None = None) -> bool:
    payload = dict(config or {})
    market_context_cfg = dict(payload.get("market_context") or {})
    return bool(
        market_context_cfg.get("enable_global_proxy_runtime", False)
        or payload.get("enable_global_proxy_runtime", False)
    )


def load_global_proxy_snapshot(config: Mapping[str, Any] | None = None) -> Dict[str, Any]:
    if not global_proxy_runtime_enabled(config):
        return {}
    return market_regime_proxy()


def macro_lines(china_macro: Dict[str, Any], global_proxy: Dict[str, Any]) -> List[str]:
    lines = []
    if china_macro:
        pmi_trend = {"rising": "回升", "falling": "回落", "stable": "持平"}.get(str(china_macro.get("pmi_trend")), "持平")
        ppi_trend = {"rising": "回升", "falling": "回落", "stable": "持平"}.get(str(china_macro.get("ppi_trend")), "持平")
        cpi_trend = {"rising": "抬升", "falling": "回落", "stable": "平稳"}.get(str(china_macro.get("cpi_trend")), "平稳")
        spread_trend = {"rising": "修复", "falling": "走弱", "stable": "持平"}.get(str(china_macro.get("m1_m2_spread_trend")), "持平")
        lines.append(
            f"制造业 PMI {china_macro['pmi']:.1f}，较前值{pmi_trend}；新订单 {china_macro.get('pmi_new_orders', 0.0):.1f}、生产 {china_macro.get('pmi_production', 0.0):.1f}。"
        )
        lines.append(
            f"PPI 同比 {china_macro.get('ppi_yoy', 0.0):.1f}% ，较前值{ppi_trend}；CPI 同比 {china_macro['cpi_monthly']:.1f}% ，价格环境{cpi_trend}。"
        )
        lines.append(
            f"M1-M2 剪刀差 {china_macro.get('m1_m2_spread', 0.0):+.1f} 个百分点，较前值{spread_trend}；社融近 3 个月均值约 {china_macro.get('social_financing_3m_avg_text', '—')}。"
        )
        lines.append(f"LPR 1Y 最近值 {china_macro['lpr_1y']:.2f}%，信用脉冲判断为 `{china_macro.get('credit_impulse', 'stable')}`。")
    if global_proxy:
        if "vix" in global_proxy:
            lines.append(f"VIX 位于 {global_proxy['vix']:.1f}。")
        if "dxy" in global_proxy and "dxy_20d_change" in global_proxy:
            lines.append(
                f"DXY 目前 {global_proxy['dxy']:.2f}，20 日变动 {global_proxy['dxy_20d_change'] * 100:+.2f}%。"
            )
        if "copper_gold_ratio" in global_proxy:
            lines.append(f"铜金比约为 {global_proxy['copper_gold_ratio']:.3f}。")
    return lines


def derive_regime_inputs(
    china_macro: Dict[str, Any],
    global_proxy: Dict[str, Any],
    monitor_rows: Optional[Sequence[Dict[str, Any]]] = None,
) -> Dict[str, Any]:
    dxy_change = float(global_proxy.get("dxy_20d_change", 0.0))
    pmi = float(china_macro.get("pmi", 50.0))
    lpr = float(china_macro.get("lpr_1y", 0.0))
    lpr_prev = float(china_macro.get("lpr_prev", lpr))
    monitor_map = {str(item.get("name", "")): item for item in monitor_rows or []}
    brent = monitor_map.get("布伦特原油", {})
    oil_5d_change = float(brent.get("return_5d", 0.0))
    oil_20d_change = float(brent.get("return_20d", 0.0))

    if lpr < lpr_prev:
        policy_stance = "easing"
    elif lpr > lpr_prev:
        policy_stance = "tightening"
    else:
        policy_stance = "neutral" if dxy_change < 0.015 else "dilemma"

    credit_impulse = str(china_macro.get("credit_impulse", "stable"))

    return {
        "pmi": pmi,
        "pmi_trend": china_macro.get("pmi_trend", "stable"),
        "pmi_new_orders": float(china_macro.get("pmi_new_orders", pmi)),
        "pmi_production": float(china_macro.get("pmi_production", pmi)),
        "demand_state": str(china_macro.get("demand_state", "stable")),
        "inventory_state": str(china_macro.get("inventory_state", "balanced")),
        "cpi": float(china_macro.get("cpi_monthly", 0.0)),
        "cpi_trend": china_macro.get("cpi_trend", "stable"),
        "ppi": float(china_macro.get("ppi_yoy", 0.0)),
        "ppi_trend": str(china_macro.get("ppi_trend", "stable")),
        "price_state": str(china_macro.get("price_state", "stable")),
        "m1_m2_spread": float(china_macro.get("m1_m2_spread", 0.0)),
        "m1_m2_spread_trend": str(china_macro.get("m1_m2_spread_trend", "stable")),
        "social_financing_3m_avg": float(china_macro.get("social_financing_3m_avg", 0.0)),
        "social_financing_prev_3m_avg": float(china_macro.get("social_financing_prev_3m_avg", 0.0)),
        "social_financing_trend": str(china_macro.get("social_financing_trend", "stable")),
        "credit_impulse": credit_impulse,
        "policy_stance": policy_stance,
        "dxy_state": "strengthening" if dxy_change > 0.015 else "weakening" if dxy_change < -0.015 else "stable",
        "oil_5d_change": oil_5d_change,
        "oil_20d_change": oil_20d_change,
    }


def extract_numbers_from_text(text: str) -> List[str]:
    patterns = re.findall(r"([0-9]+(?:\.[0-9]+)?[%万亿亿元万千]*)", text)
    unique = []
    for item in patterns:
        if item not in unique:
            unique.append(item)
    return unique[:6]
