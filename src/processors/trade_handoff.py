"""Shared trade handoff helpers for portfolio preflight messaging."""

from __future__ import annotations

from typing import Any, Dict, Mapping

from src.processors.horizon import infer_horizon_code_from_period


def reference_price_text(asset_type: str, reference_price: Any = None) -> str:
    try:
        price = float(reference_price)
    except (TypeError, ValueError):
        price = 0.0
    if price > 0:
        return f"{price:.4f}"
    return "最新净值" if asset_type == "cn_fund" else "最新价"


def portfolio_whatif_handoff(
    *,
    symbol: str,
    horizon: Mapping[str, Any] | None = None,
    direction: str = "",
    asset_type: str = "",
    reference_price: Any = None,
) -> Dict[str, str]:
    horizon_payload = dict(horizon or {})
    label = str(horizon_payload.get("label", "")).strip() or "观察期"
    code = str(horizon_payload.get("code", "")).strip() or str(infer_horizon_code_from_period(label) or "watch")
    trade_action = "sell" if any(token in str(direction or "") for token in ("卖", "减仓", "止盈", "止损", "回收")) else "buy"
    price_text = reference_price_text(asset_type, reference_price)
    command = f"portfolio whatif {trade_action} {symbol} {price_text} 计划金额"

    if code == "short_term":
        summary = f"把它当 `{label}` 的交易仓处理：先预演首笔金额落下去后，仓位、执行成本和止损纪律是否还成立。"
    elif code == "swing":
        summary = f"把它当 `{label}` 的波段仓处理：先看首笔落下去后，单票权重、行业暴露和后续第二笔空间还剩多少。"
    elif code == "position_trade":
        summary = f"把它当 `{label}` 的配置仓处理：落单前先看加仓后是否仍在组合风险预算和行业/地区上限内。"
    elif code == "long_term_allocation":
        summary = f"把它当 `{label}` 的底仓处理：先看长期目标权重和风险预算能否承受，而不是只盯一次下单的短期波动。"
    else:
        summary = f"当前更像 `{label}`，先别急着落单；如果你坚持试仓，至少先预演这笔单会不会把组合推过上限。"
    return {"summary": summary, "command": command}
