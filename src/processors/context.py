"""Macro and market context snapshots used by Phase 2 commands."""

from __future__ import annotations

from typing import Any, Dict, List

import pandas as pd

from src.collectors.macro_cn import ChinaMacroCollector
from src.utils.market import market_regime_proxy


def _last_valid_value(frame: pd.DataFrame, column: str) -> float:
    series = pd.to_numeric(frame[column], errors="coerce").dropna()
    if series.empty:
        raise ValueError(f"No valid values for column: {column}")
    return float(series.iloc[-1])


def load_china_macro_snapshot(config: Dict[str, Any]) -> Dict[str, float]:
    collector = ChinaMacroCollector(config)
    pmi_frame = collector.get_pmi()
    pmi = float(pd.to_numeric(pmi_frame["制造业-指数"], errors="coerce").dropna().iloc[0])
    pmi_prev = float(pd.to_numeric(pmi_frame["制造业-指数"], errors="coerce").dropna().iloc[1])

    cpi_frame = collector.get_cpi()
    cpi = _last_valid_value(cpi_frame, "今值")

    lpr_frame = collector.get_lpr()
    lpr_series = pd.to_numeric(lpr_frame["LPR1Y"], errors="coerce").dropna()
    lpr = float(lpr_series.iloc[-1])
    lpr_prev = float(lpr_series.iloc[-2]) if len(lpr_series) >= 2 else lpr

    return {
        "pmi": pmi,
        "pmi_prev": pmi_prev,
        "cpi_monthly": cpi,
        "lpr_1y": lpr,
        "lpr_prev": lpr_prev,
    }


def load_global_proxy_snapshot() -> Dict[str, Any]:
    return market_regime_proxy()


def macro_lines(china_macro: Dict[str, Any], global_proxy: Dict[str, Any]) -> List[str]:
    lines = []
    if china_macro:
        pmi_trend = "回升" if china_macro["pmi"] >= china_macro["pmi_prev"] else "回落"
        lines.append(f"中国制造业 PMI {china_macro['pmi']:.1f}，较前值 {pmi_trend}。")
        lines.append(f"中国 CPI 月率最近值 {china_macro['cpi_monthly']:.1f}%。")
        lines.append(f"LPR 1Y 最近值 {china_macro['lpr_1y']:.2f}%。")
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
