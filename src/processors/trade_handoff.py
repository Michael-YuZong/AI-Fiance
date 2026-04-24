"""Shared trade handoff helpers for portfolio preflight messaging."""

from __future__ import annotations

from datetime import datetime, time
from typing import Any, Dict, Mapping
from zoneinfo import ZoneInfo

from src.processors.horizon import horizon_family_code, infer_horizon_code_from_period

_SH_TZ = ZoneInfo("Asia/Shanghai")


def reference_price_text(asset_type: str, reference_price: Any = None) -> str:
    try:
        price = float(reference_price)
    except (TypeError, ValueError):
        price = 0.0
    if price > 0:
        return f"{price:.4f}"
    return "最新净值" if asset_type == "cn_fund" else "最新价"


def _parse_generated_at(generated_at: Any = None) -> datetime:
    if isinstance(generated_at, datetime):
        if generated_at.tzinfo is None:
            return generated_at.replace(tzinfo=_SH_TZ)
        return generated_at.astimezone(_SH_TZ)

    text = str(generated_at or "").strip()
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d %H:%M", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt).replace(tzinfo=_SH_TZ)
        except ValueError:
            continue
    return datetime.now(_SH_TZ)


def _horizon_code(horizon: Mapping[str, Any] | None) -> str:
    payload = dict(horizon or {})
    family_code = horizon_family_code(payload, default="")
    if family_code:
        return family_code
    label = str(payload.get("label", "")).strip()
    return str(infer_horizon_code_from_period(label) or "watch")


def _asset_family(asset_type: str) -> str:
    if asset_type in {"cn_stock", "cn_etf"}:
        return "cn_equity"
    if asset_type == "cn_fund":
        return "cn_fund"
    if asset_type == "hk":
        return "hk_equity"
    if asset_type == "us":
        return "us_equity"
    return "generic"


def _phase_code(asset_type: str, as_of: datetime) -> str:
    family = _asset_family(asset_type)
    clock = as_of.time()
    is_weekend = as_of.weekday() >= 5

    if family == "cn_equity":
        if is_weekend:
            return "off_day"
        if clock < time(9, 15):
            return "pre_open"
        if clock < time(9, 30):
            return "pre_session"
        if clock < time(11, 30):
            return "live_session"
        if clock < time(13, 0):
            return "midday_break"
        if clock < time(14, 40):
            return "live_session"
        if clock < time(15, 0):
            return "late_session"
        return "post_close"

    if family == "hk_equity":
        if is_weekend:
            return "off_day"
        if clock < time(9, 30):
            return "pre_open"
        if clock < time(12, 0):
            return "live_session"
        if clock < time(13, 0):
            return "midday_break"
        if clock < time(15, 40):
            return "live_session"
        if clock < time(16, 0):
            return "late_session"
        return "post_close"

    if family == "cn_fund":
        if is_weekend:
            return "off_day"
        if clock < time(14, 30):
            return "open_day"
        if clock < time(15, 0):
            return "cutoff_window"
        return "post_cutoff"

    if family == "us_equity":
        if clock < time(18, 0):
            return "next_us_session"
        if clock < time(21, 30):
            return "pre_us_session"
        if clock >= time(21, 30) or clock < time(5, 0):
            return "us_session"
        return "post_us_close"

    return "generic"


def recommendation_timing_context(
    *,
    asset_type: str = "",
    horizon: Mapping[str, Any] | None = None,
    generated_at: Any = None,
) -> Dict[str, str]:
    as_of = _parse_generated_at(generated_at)
    date_text = as_of.strftime("%Y-%m-%d")
    label = str(dict(horizon or {}).get("label", "")).strip() or "观察期"
    code = _horizon_code(horizon)
    phase = _phase_code(asset_type, as_of)
    family = _asset_family(asset_type)
    is_watch = code == "watch"
    is_short = code == "short_term"
    is_slow = code in {"swing", "position_trade", "long_term_allocation"}

    if family in {"cn_equity", "hk_equity"}:
        if phase in {"pre_open", "pre_session"}:
            headline_scope = "今天"
            decision_scope = "今天的交易计划"
            if is_short:
                summary = f"这版判断生成于 `{date_text}` 交易前时段，默认先对应今天的交易计划；短线更适合等早段确认后再执行，不要在交易前把计划直接打满。"
            elif is_watch:
                summary = f"这版判断生成于 `{date_text}` 交易前时段，默认先作为今天的观察名单理解；先等早段确认，不需要在交易开始前抢着给动作。"
            else:
                summary = f"这版判断生成于 `{date_text}` 交易前时段，默认也先按今天的计划理解；但这类 `{label}` 更看分批和回踩确认，不必把交易前几分钟当成关键胜负手。"
        elif phase == "midday_break":
            headline_scope = "今天"
            decision_scope = "今天午后或后续交易时段的计划"
            if is_short:
                summary = f"这版判断生成于 `{date_text}` 午间休市时段，默认对应今天午后或后续交易时段；短线更适合把它当成午后计划，不要把午休时的判断直接当成已经成交的机会。"
            elif is_watch:
                summary = f"这版判断生成于 `{date_text}` 午间休市时段，默认先作为今天午后的观察名单理解；先等午后确认，不急着在休市阶段把它升级成正式动作。"
            else:
                summary = f"这版判断生成于 `{date_text}` 午间休市时段，今天能做的是把分批计划排好；这类 `{label}` 更看午后和后续几个交易日的承接，不靠中午这段时间定输赢。"
        elif phase == "late_session":
            headline_scope = "今天已接近收盘，下一个交易日"
            decision_scope = "下一个交易日的计划"
            if is_short:
                summary = f"这版判断生成于 `{date_text}` 尾段，离收盘已近；短线执行更适合顺延到下一个交易日，不把最后一段时间当成必须完成的窗口。"
            elif is_watch:
                summary = f"这版判断生成于 `{date_text}` 尾段，观察结论可以继续保留，但真正执行更适合留到下一个交易日。"
            else:
                summary = f"这版判断生成于 `{date_text}` 尾段，执行上更适合顺延到下一个交易日；但这类 `{label}` 判断主要影响明天怎么分批，不会因为快收盘就失效。"
        elif phase in {"post_close", "off_day"}:
            headline_scope = "今天已收盘，下一个交易日"
            decision_scope = "下一个交易日的计划"
            if is_short:
                summary = f"这版判断生成于 `{date_text}` 收盘后或非交易时段；短线执行默认顺延到下一个交易日，不把今天收盘后的判断误读成还能当天成交的机会。"
            elif is_watch:
                summary = f"这版判断生成于 `{date_text}` 收盘后或非交易时段；观察结论自然顺延到下一个交易日，不需要强行理解成今天必须动。"
            else:
                summary = f"这版判断生成于 `{date_text}` 收盘后或非交易时段；执行上默认顺延到下一个交易日，但这类 `{label}` 判断主要决定明天怎么开第一笔，不会因为今天收盘就失效。"
        else:
            headline_scope = "今天"
            decision_scope = "今天剩余交易时段的计划"
            if is_short:
                summary = f"这版判断生成于 `{date_text}` 交易时段内，默认对应今天剩余交易时段；短线更适合等回踩或确认，不适合把已经拉起来的一脚直接当成追价理由。"
            elif is_watch:
                summary = f"这版判断生成于 `{date_text}` 交易时段内，默认对应今天剩余交易时段的观察；先看确认信号是否补齐，不急着把它升级成正式动作。"
            else:
                summary = f"这版判断生成于 `{date_text}` 交易时段内，今天能做的是先按计划分批；但这类 `{label}` 更看后续几个交易日的承接和节奏，不靠一两个小时定输赢。"
        return {
            "headline_scope": headline_scope,
            "decision_scope": decision_scope,
            "summary": summary,
            "phase": phase,
            "date": date_text,
        }

    if family == "cn_fund":
        if phase == "open_day":
            headline_scope = "今天"
            decision_scope = "今天的申赎决策"
            if is_watch:
                summary = f"这版判断生成于 `{date_text}` 申赎时段内，默认先对应今天的申赎决策；当前更适合按申赎节奏继续观察，不按白天的情绪波动抢着下判断。"
            elif is_short:
                summary = f"这版判断生成于 `{date_text}` 申赎时段内，默认对应今天的申赎决策；场外基金按收盘后确认净值，这类短线判断也更适合按申赎节奏理解，不按分时涨跌去追。"
            else:
                summary = f"这版判断生成于 `{date_text}` 申赎时段内，默认对应今天的申赎决策；场外基金仍按收盘后确认净值，这类 `{label}` 更重要的是申赎节奏和分批，不是白天某个瞬时价格。"
        elif phase == "cutoff_window":
            headline_scope = "今天临近申赎截止，更像下一个开放日"
            decision_scope = "今天最后申赎窗口或下一个开放日的计划"
            if is_slow:
                summary = f"这版判断生成于 `{date_text}` 临近 15:00 的申赎截止窗口；执行上更适合把它理解成今天最后窗口或下一个开放日的分批计划，而不是临时赶在截止前重仓处理。"
            else:
                summary = f"这版判断生成于 `{date_text}` 临近 15:00 的申赎截止窗口；如果还想按今天处理，时间已经不宽裕，更适合把它当成今天最后窗口或下一个开放日的计划。"
        else:
            headline_scope = "今天申赎时点已过，下一个开放日"
            decision_scope = "下一个开放日的申赎计划"
            if is_slow:
                summary = f"这版判断生成于 `{date_text}` 15:00 后或非开放时段；执行上默认顺延到下一个开放日，但这类 `{label}` 判断主要影响下一次申赎怎么分批，不会因为今天截止就失效。"
            else:
                summary = f"这版判断生成于 `{date_text}` 15:00 后或非开放时段，今天按当日净值申赎的时点基本过去；执行上默认顺延到下一个开放日。"
        return {
            "headline_scope": headline_scope,
            "decision_scope": decision_scope,
            "summary": summary,
            "phase": phase,
            "date": date_text,
        }

    if family == "us_equity":
        headline_scope = "下一次美股交易时段"
        decision_scope = "下一次美股交易时段的计划"
        if is_watch:
            summary = f"这版判断生成于 `{date_text}`，默认对应下一次可交易的美股时段；如果你现在在中国白天看，通常就是今晚，当前更适合作为观察名单而不是立即动作。"
        elif is_short:
            summary = f"这版判断生成于 `{date_text}`，默认对应下一次可交易的美股时段；如果你现在在中国白天看，通常就是今晚。短线仍要等确认，不把非交易时段的判断直接当成立即成交机会。"
        else:
            summary = f"这版判断生成于 `{date_text}`，默认对应下一次可交易的美股时段；对这类 `{label}` 来说，关键是今晚或后续几个交易日怎么分批，不是一定要在第一个时点立刻动作。"
        return {
            "headline_scope": headline_scope,
            "decision_scope": decision_scope,
            "summary": summary,
            "phase": phase,
            "date": date_text,
        }

    return {
        "headline_scope": "当前这版",
        "decision_scope": "当前这版计划",
        "summary": f"这版判断生成于 `{date_text}`；执行上优先按当前可交易窗口和 `{label}` 的节奏理解，不把一条观点直接等同成必须立刻完成的动作。",
        "phase": phase,
        "date": date_text,
    }


def portfolio_whatif_handoff(
    *,
    symbol: str,
    horizon: Mapping[str, Any] | None = None,
    direction: str = "",
    asset_type: str = "",
    reference_price: Any = None,
    generated_at: Any = None,
) -> Dict[str, str]:
    horizon_payload = dict(horizon or {})
    label = str(horizon_payload.get("label", "")).strip() or "观察期"
    code = _horizon_code(horizon_payload)
    trade_action = "sell" if any(token in str(direction or "") for token in ("卖", "减仓", "止盈", "止损", "回收")) else "buy"
    price_text = reference_price_text(asset_type, reference_price)
    command = f"portfolio whatif {trade_action} {symbol} {price_text} 计划金额"
    timing = recommendation_timing_context(
        asset_type=asset_type,
        horizon=horizon_payload,
        generated_at=generated_at,
    )

    if code == "short_term":
        summary = f"把 {symbol} 当 `{label}` 的交易仓处理：先预演首笔金额落下去后，仓位、执行成本和止损纪律是否还成立。"
    elif code == "swing":
        summary = f"把 {symbol} 当 `{label}` 的波段仓处理：先看首笔落下去后，单票权重、行业暴露和后续第二笔空间还剩多少。"
    elif code == "position_trade":
        summary = f"把 {symbol} 当 `{label}` 的配置仓处理：落单前先看加仓后是否仍在组合风险预算和行业/地区上限内。"
    elif code == "long_term_allocation":
        summary = f"把 {symbol} 当 `{label}` 的底仓处理：先看长期目标权重和风险预算能否承受，而不是只盯一次下单的短期波动。"
    else:
        summary = f"{symbol} 当前更像 `{label}`，先别急着落单；如果你坚持试仓，至少先预演这笔单会不会把组合推过上限。"
    return {
        "summary": summary,
        "command": command,
        "timing_summary": timing["summary"],
        "decision_scope": timing["decision_scope"],
        "headline_scope": timing["headline_scope"],
    }
