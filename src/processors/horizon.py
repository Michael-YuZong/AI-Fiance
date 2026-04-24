"""Shared holding-horizon contracts across analysis, trade planning, and review."""

from __future__ import annotations

import re
from typing import Any, Dict, Mapping, Optional


_HORIZON_FAMILY_LIBRARY: Dict[str, Dict[str, str]] = {
    "watch": {
        "label": "观察期",
        "default_code": "watch_wait_for_confirmation",
        "setup_code": "wait_for_confirmation",
        "setup_label": "待确认观察",
        "style": "先等催化、趋势或风险收益比进一步确认，不急着把它定义成短线执行仓或长线配置仓。",
        "fit_reason": "当前信号还没共振到足以支撑正式动作，继续观察比仓促出手更重要。",
        "misfit_reason": "现在不适合直接按短线执行仓或长线配置仓去理解。",
    },
    "short_term": {
        "label": "短线交易（3-10日）",
        "default_code": "short_term_tactical_execution",
        "setup_code": "tactical_execution",
        "setup_label": "短线战术执行",
        "style": "更看催化、趋势和执行节奏，适合盯右侧确认和止损，不适合当成长线底仓。",
        "fit_reason": "当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。",
        "misfit_reason": "现在不适合直接当成长线底仓，一旦催化和强势股状态失效要更快处理。",
    },
    "swing": {
        "label": "波段跟踪（2-6周）",
        "default_code": "swing_staged_followthrough",
        "setup_code": "staged_followthrough",
        "setup_label": "波段续航跟踪",
        "style": "更适合按几周级别的波段节奏去跟踪，等确认和回踩，不靠单日冲动去追。",
        "fit_reason": "趋势、轮动或风险收益比已经有基础，但更依赖未来几周节奏，而不是长周期基本面完全兑现。",
        "misfit_reason": "现在不适合把它当长期底仓，也不适合只按隔夜消息去赌超短。",
    },
    "position_trade": {
        "label": "中线配置（1-3月）",
        "default_code": "position_trade_core_accumulation",
        "setup_code": "core_accumulation",
        "setup_label": "中线分批配置",
        "style": "更像 1-3 个月的分批配置或波段跟踪，不按隔日涨跌去做快进快出。",
        "fit_reason": "基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。",
        "misfit_reason": "现在不适合当成纯隔夜交易，也还没强到可以长期不复核地持有一年以上。",
    },
    "long_term_allocation": {
        "label": "长线配置（6-12月）",
        "default_code": "long_term_core_allocation",
        "setup_code": "core_allocation",
        "setup_label": "长线核心配置",
        "style": "更适合作为中长期底仓来跟踪，允许短线波动，但要持续复核主线、基本面和风险预算。",
        "fit_reason": "基本面、风险收益和主线顺风更完整，持有逻辑不只依赖眼前一两周的催化。",
        "misfit_reason": "现在不适合按纯短线追价来理解，短线节奏错了也不能破坏长线仓位纪律。",
    },
}

_HORIZON_CODE_LIBRARY: Dict[str, Dict[str, str]] = {
    "watch_wait_for_confirmation": {
        "family_code": "watch",
        "setup_code": "wait_for_confirmation",
        "setup_label": "待确认观察",
    },
    "watch_high_stopout_risk": {
        "family_code": "watch",
        "setup_code": "high_stopout_risk",
        "setup_label": "高止损触发风险",
    },
    "watch_historical_edge_without_confirmation": {
        "family_code": "watch",
        "setup_code": "historical_edge_without_confirmation",
        "setup_label": "历史优势未转成右侧确认",
    },
    "watch_crowded_mainline_consolidation": {
        "family_code": "watch",
        "setup_code": "crowded_mainline_consolidation",
        "setup_label": "高位拥挤主线分歧",
    },
    "watch_fundamental_gap": {
        "family_code": "watch",
        "setup_code": "fundamental_gap",
        "setup_label": "基本面约束未解",
    },
    "watch_rotation_lag": {
        "family_code": "watch",
        "setup_code": "rotation_lag",
        "setup_label": "轮动承接未回流",
    },
    "watch_catalyst_without_price_confirmation": {
        "family_code": "watch",
        "setup_code": "catalyst_without_price_confirmation",
        "setup_label": "催化先行但价格未确认",
    },
    "watch_bottom_repair": {
        "family_code": "watch",
        "setup_code": "bottom_repair",
        "setup_label": "底部修复等待确认",
    },
    "watch_defensive_without_catalyst": {
        "family_code": "watch",
        "setup_code": "defensive_without_catalyst",
        "setup_label": "防守属性在但缺催化",
    },
    "watch_structure_without_new_catalyst": {
        "family_code": "watch",
        "setup_code": "structure_without_new_catalyst",
        "setup_label": "结构未坏但缺新增催化",
    },
    "watch_poor_reward_risk": {
        "family_code": "watch",
        "setup_code": "poor_reward_risk",
        "setup_label": "赔率与风控不占优",
    },
    "watch_macro_headwind": {
        "family_code": "watch",
        "setup_code": "macro_headwind",
        "setup_label": "宏观逆风观察",
    },
    "watch_low_sample_confidence": {
        "family_code": "watch",
        "setup_code": "low_sample_confidence",
        "setup_label": "样本置信度偏低",
    },
    "watch_risk_disposal": {
        "family_code": "watch",
        "setup_code": "risk_disposal",
        "setup_label": "风险处置观察",
    },
    "short_term_tactical_execution": {
        "family_code": "short_term",
        "setup_code": "tactical_execution",
        "setup_label": "短线战术执行",
    },
    "short_term_catalyst_breakout": {
        "family_code": "short_term",
        "setup_code": "catalyst_breakout",
        "setup_label": "催化驱动突破",
    },
    "short_term_overbought_followthrough": {
        "family_code": "short_term",
        "setup_code": "overbought_followthrough",
        "setup_label": "强势过热后的跟随",
    },
    "swing_staged_followthrough": {
        "family_code": "swing",
        "setup_code": "staged_followthrough",
        "setup_label": "波段续航跟踪",
    },
    "swing_mainline_rotation_followthrough": {
        "family_code": "swing",
        "setup_code": "mainline_rotation_followthrough",
        "setup_label": "主线轮动延续",
    },
    "swing_crowded_mainline_followthrough": {
        "family_code": "swing",
        "setup_code": "crowded_mainline_followthrough",
        "setup_label": "高位主线强势整理",
    },
    "swing_fundamental_resilience_with_volatility": {
        "family_code": "swing",
        "setup_code": "fundamental_resilience_with_volatility",
        "setup_label": "基本面站住但波动偏大",
    },
    "swing_repair_after_drawdown": {
        "family_code": "swing",
        "setup_code": "repair_after_drawdown",
        "setup_label": "回撤后修复跟踪",
    },
    "swing_right_side_confirmation": {
        "family_code": "swing",
        "setup_code": "right_side_confirmation",
        "setup_label": "右侧确认波段",
    },
    "position_trade_core_accumulation": {
        "family_code": "position_trade",
        "setup_code": "core_accumulation",
        "setup_label": "中线分批配置",
    },
    "position_trade_hold_not_chase": {
        "family_code": "position_trade",
        "setup_code": "hold_not_chase",
        "setup_label": "持有优于追高",
    },
    "position_trade_thesis_stated": {
        "family_code": "position_trade",
        "setup_code": "thesis_stated",
        "setup_label": "按 thesis 中线执行",
    },
    "long_term_core_allocation": {
        "family_code": "long_term_allocation",
        "setup_code": "core_allocation",
        "setup_label": "长线核心配置",
    },
    "long_term_quality_compounding": {
        "family_code": "long_term_allocation",
        "setup_code": "quality_compounding",
        "setup_label": "质量复利型持有",
    },
    "long_term_thesis_stated": {
        "family_code": "long_term_allocation",
        "setup_code": "thesis_stated",
        "setup_label": "按 thesis 长线执行",
    },
    "short_term_thesis_stated": {
        "family_code": "short_term",
        "setup_code": "thesis_stated",
        "setup_label": "按 thesis 短线执行",
    },
    "swing_thesis_stated": {
        "family_code": "swing",
        "setup_code": "thesis_stated",
        "setup_label": "按 thesis 波段执行",
    },
    "watch_thesis_stated": {
        "family_code": "watch",
        "setup_code": "thesis_stated",
        "setup_label": "按 thesis 继续观察",
    },
}

_HORIZON_FAMILY_CODES = frozenset(_HORIZON_FAMILY_LIBRARY.keys())


_HORIZON_EXPRESSION_LIBRARY: Dict[str, Dict[str, Any]] = {
    "wait_for_confirmation": {
        "write_as": "待确认观察，不急着翻译成交易动作",
        "prompt_hint": "把它写成待确认观察：强调需要趋势、催化或风险收益比进一步确认，不要替规则层升级动作。",
        "forbidden_terms": ("已确认启动", "正式买点", "低门槛机会"),
    },
    "high_stopout_risk": {
        "write_as": "高止损触发风险下的观察",
        "prompt_hint": "把它写成高止损触发风险：重点写噪声止损和执行陷阱，不要把窄止损包装成高赔率。",
        "forbidden_terms": ("低风险试错", "舒服买点", "高赔率确定性"),
    },
    "historical_edge_without_confirmation": {
        "write_as": "历史样本不差但当前确认不足",
        "prompt_hint": "把它写成历史优势还没转成当下右侧确认：历史胜率只能辅助，不能直接变成今天的出手理由。",
        "forbidden_terms": ("高胜率出手点", "已经验证", "现在就能下手"),
    },
    "crowded_mainline_consolidation": {
        "write_as": "高位拥挤主线里的分歧消化",
        "prompt_hint": "把它写成高位拥挤主线分歧：方向未必坏，但位置、假突破、压力位或情绪过热要求等拥挤消化后的再确认。",
        "forbidden_terms": ("修复早期", "超跌反弹", "低位启动", "底部修复"),
    },
    "fundamental_gap": {
        "write_as": "基本面约束未解的观察",
        "prompt_hint": "把它写成基本面约束未解：先写支撑不足和验证条件，不要用跌幅或题材替代基本面确认。",
        "forbidden_terms": ("长期底仓", "基本面已确认", "越跌越买"),
    },
    "rotation_lag": {
        "write_as": "基本面未坏但轮动承接未回流",
        "prompt_hint": "把它写成轮动承接未回流：基本面可以保留观察价值，但资金和相对强弱没回来前不能写成趋势重启。",
        "forbidden_terms": ("趋势重启", "资金已经回流", "主线回归"),
    },
    "catalyst_without_price_confirmation": {
        "write_as": "催化先行但价格未确认",
        "prompt_hint": "把它写成催化线索先亮灯：强调价格、量能和相对强弱还要确认，不要把单条消息写成已确认突破。",
        "forbidden_terms": ("已经确认突破", "正式启动", "催化已兑现成趋势"),
    },
    "bottom_repair": {
        "write_as": "底部修复等待确认",
        "prompt_hint": "把它写成底部修复等待确认：可以写修复，但必须同时写等待 MA、MACD、量能或相对强弱确认。",
        "forbidden_terms": ("主线加速", "高位强势整理", "趋势已经重启"),
    },
    "defensive_without_catalyst": {
        "write_as": "防守属性在但缺新增催化",
        "prompt_hint": "把它写成防守观察：风险属性可能较稳，但缺催化时不要写成进攻方向或主攻仓。",
        "forbidden_terms": ("进攻型机会", "主攻线", "催化充足"),
    },
    "structure_without_new_catalyst": {
        "write_as": "结构未坏但缺新增催化",
        "prompt_hint": "把它写成结构未坏但缺新触发点：保留观察，不要把结构尚可写成高确定性右侧机会。",
        "forbidden_terms": ("强催化", "高确定性右侧机会", "主攻线"),
    },
    "poor_reward_risk": {
        "write_as": "赔率与风控不占优",
        "prompt_hint": "把它写成赔率和风控不占优：重点写风险边界不清，不要为了结论好看硬写高赔率。",
        "forbidden_terms": ("高赔率", "风险可控", "低风险"),
    },
    "macro_headwind": {
        "write_as": "宏观或风格逆风下的观察",
        "prompt_hint": "把它写成方向未必坏但环境逆风：写清宏观/风格因素如何压制仓位和执行节奏。",
        "forbidden_terms": ("环境顺风", "可以提杠杆", "进攻仓"),
    },
    "low_sample_confidence": {
        "write_as": "样本置信度偏低的线索观察",
        "prompt_hint": "把它写成低样本置信度：强调线索级观察和后续验证，不要写成统计上已经稳健。",
        "forbidden_terms": ("样本充分", "统计稳健", "高确定性"),
    },
    "risk_disposal": {
        "write_as": "风险处置，不是新进攻周期",
        "prompt_hint": "把它写成风险处置：重点写减仓、止损或复核，不要把卖出/降风险预演改写成新的买入逻辑。",
        "forbidden_terms": ("新买点", "重新进攻", "加仓窗口"),
    },
    "tactical_execution": {
        "write_as": "短线战术执行",
        "prompt_hint": "把它写成短线战术执行：突出触发、止损和兑现纪律，不要延展成长线底仓叙事。",
        "forbidden_terms": ("长线底仓", "长期无需复核", "越涨越配"),
    },
    "catalyst_breakout": {
        "write_as": "催化驱动突破",
        "prompt_hint": "把它写成催化驱动的短线突破：强调事件兑现和右侧连续性，别写成长期确定性已经兑现。",
        "forbidden_terms": ("长期确定性已兑现", "无视回撤", "底仓配置"),
    },
    "overbought_followthrough": {
        "write_as": "强势过热后的纪律跟随",
        "prompt_hint": "把它写成强势但偏热的短线跟随：可以承认强势，但必须写追价风险、止损和回落处理。",
        "forbidden_terms": ("低位启动", "舒服买点", "无脑追"),
    },
    "staged_followthrough": {
        "write_as": "波段续航跟踪",
        "prompt_hint": "把它写成几周级波段跟踪：强调回踩确认和分批节奏，不要写成隔夜消息交易或长期底仓。",
        "forbidden_terms": ("隔夜赌消息", "长期底仓", "一次打满"),
    },
    "mainline_rotation_followthrough": {
        "write_as": "主线轮动延续",
        "prompt_hint": "把它写成主线轮动延续：优势来自催化和相对强弱共振，但仍要写能否继续得到资金确认。",
        "forbidden_terms": ("低位修复", "长期胜率已定", "无条件追高"),
    },
    "crowded_mainline_followthrough": {
        "write_as": "高位主线强势整理",
        "prompt_hint": "把它写成高位主线强势整理：承认主线和强弱仍在，同时强调拥挤、压力位和分歧消化。",
        "forbidden_terms": ("修复早期", "低位启动", "底部反转", "舒服买点"),
    },
    "fundamental_resilience_with_volatility": {
        "write_as": "基本面站住但波动偏大",
        "prompt_hint": "把它写成基本面韧性和波动约束并存：可分批确认，不能一次性重仓。",
        "forbidden_terms": ("一次打满", "低波无风险", "确定性底仓"),
    },
    "repair_after_drawdown": {
        "write_as": "回撤后修复跟踪",
        "prompt_hint": "把它写成回撤后的修复跟踪：可以写修复，但必须写确认过程和仓位节奏。",
        "forbidden_terms": ("趋势已经重启", "无条件反转", "直接追高"),
    },
    "right_side_confirmation": {
        "write_as": "右侧确认后的波段跟踪",
        "prompt_hint": "把它写成右侧确认刚建立：重点看回踩承接和相对强弱延续，不要扩写成长线确定性。",
        "forbidden_terms": ("长线确定性", "长期底仓", "不用复核"),
    },
    "core_accumulation": {
        "write_as": "中线分批配置",
        "prompt_hint": "把它写成中线分批配置：强调分批、复核和风险预算，不要写成隔日胜负或满仓押注。",
        "forbidden_terms": ("隔日定成败", "满仓押注", "超短套利"),
    },
    "hold_not_chase": {
        "write_as": "持有优于追高",
        "prompt_hint": "把它写成中线逻辑还在但不适合追高：持有和等待回踩优先，别把主线成立写成立即追价。",
        "forbidden_terms": ("立即追价", "一次打满", "无视回踩"),
    },
    "thesis_stated": {
        "write_as": "沿用 thesis 周期，但仍需复核",
        "prompt_hint": "把它写成按已有 thesis 周期执行：可以沿用原计划，但要保留复核条件，不能把旧 thesis 写成无条件承诺。",
        "forbidden_terms": ("无条件持有", "不用复核", "机械执行"),
    },
    "core_allocation": {
        "write_as": "长线核心配置",
        "prompt_hint": "把它写成长线核心配置：强调主线、基本面和风险预算的持续复核，不要写成短线追价。",
        "forbidden_terms": ("短线追价", "隔夜交易", "无需风控"),
    },
    "quality_compounding": {
        "write_as": "质量复利型持有",
        "prompt_hint": "把它写成质量复利型中长期持有：允许短线波动，但必须持续复核主线和基本面。",
        "forbidden_terms": ("短线套利", "不看基本面", "无需复核"),
    },
}


def _horizon_family_from_payload(payload: Mapping[str, Any]) -> str:
    family_code = str(payload.get("family_code", "") or "").strip()
    if family_code in _HORIZON_FAMILY_CODES:
        return family_code
    code = str(payload.get("code", "") or "").strip()
    if code in _HORIZON_FAMILY_CODES:
        return code
    if code in _HORIZON_CODE_LIBRARY:
        return str(_HORIZON_CODE_LIBRARY[code]["family_code"])
    return "watch"


def _setup_from_payload(payload: Mapping[str, Any]) -> tuple[str, str]:
    setup_code = str(payload.get("setup_code", "") or "").strip()
    setup_label = str(payload.get("setup_label", "") or "").strip()
    code = str(payload.get("code", "") or "").strip()
    if (not setup_code or not setup_label) and code in _HORIZON_CODE_LIBRARY:
        meta = _HORIZON_CODE_LIBRARY[code]
        setup_code = setup_code or str(meta.get("setup_code", "") or "")
        setup_label = setup_label or str(meta.get("setup_label", "") or "")
    family_code = _horizon_family_from_payload(payload)
    if (not setup_code or not setup_label) and family_code in _HORIZON_FAMILY_LIBRARY:
        meta = _HORIZON_FAMILY_LIBRARY[family_code]
        setup_code = setup_code or str(meta.get("setup_code", "") or "")
        setup_label = setup_label or str(meta.get("setup_label", "") or "")
    return setup_code, setup_label


def build_horizon_expression_packet(horizon: Mapping[str, Any] | None) -> Dict[str, Any]:
    """Build the LLM-facing wording contract for a horizon profile."""
    payload = dict(horizon or {})
    if not any(str(payload.get(key, "") or "").strip() for key in ("code", "family_code", "label", "setup_code", "setup_label")):
        return {}
    setup_code, setup_label = _setup_from_payload(payload)
    family_code = _horizon_family_from_payload(payload)
    meta = dict(_HORIZON_EXPRESSION_LIBRARY.get(setup_code) or {})
    if not meta:
        label = setup_label or str(payload.get("label", "") or "").strip() or "当前周期"
        meta = {
            "write_as": label,
            "prompt_hint": f"按 `{label}` 组织周期表达，只能润色判断层，不能改变动作方向或推荐等级。",
            "forbidden_terms": ("改变推荐等级", "改写动作方向"),
        }
    return {
        "contract_version": "horizon_expression.v1",
        "source": "rules_to_llm_expression",
        "horizon_code": str(payload.get("code", "") or "").strip(),
        "family_code": family_code,
        "setup_code": setup_code,
        "setup_label": setup_label,
        "label": str(payload.get("label", "") or "").strip(),
        "write_as": str(meta.get("write_as", "") or "").strip(),
        "prompt_hint": str(meta.get("prompt_hint", "") or "").strip(),
        "forbidden_terms": [str(item).strip() for item in tuple(meta.get("forbidden_terms") or ()) if str(item).strip()],
    }


def _attach_expression_fields(horizon: Mapping[str, str]) -> Dict[str, str]:
    result = dict(horizon)
    packet = build_horizon_expression_packet(result)
    if not packet:
        return result
    result["expression_contract"] = str(packet.get("contract_version", "") or "")
    result["expression_source"] = str(packet.get("source", "") or "")
    result["expression_prompt_hint"] = str(packet.get("prompt_hint", "") or "")
    forbidden_terms = [str(item).strip() for item in list(packet.get("forbidden_terms") or []) if str(item).strip()]
    result["expression_forbidden_terms"] = " / ".join(forbidden_terms)
    return result


def _contract(
    family_code: str,
    code: str,
    label: str,
    style: str,
    fit_reason: str,
    misfit_reason: str,
    *,
    setup_code: str = "",
    setup_label: str = "",
    source: str = "",
) -> Dict[str, str]:
    return _attach_expression_fields({
        "code": code,
        "family_code": family_code,
        "label": label,
        "setup_code": setup_code,
        "setup_label": setup_label,
        "style": style,
        "fit_reason": fit_reason,
        "misfit_reason": misfit_reason,
        "source": source,
    })


def _base_contract(code: str, *, source: str = "") -> Dict[str, str]:
    meta = dict(_HORIZON_FAMILY_LIBRARY[code])
    return _contract(
        code,
        meta["default_code"],
        meta["label"],
        meta["style"],
        meta["fit_reason"],
        meta["misfit_reason"],
        setup_code=meta["setup_code"],
        setup_label=meta["setup_label"],
        source=source,
    )


def horizon_family_code(horizon: Mapping[str, Any] | None, default: str = "watch") -> str:
    payload = dict(horizon or {})
    family_code = str(payload.get("family_code", "")).strip()
    if family_code in _HORIZON_FAMILY_CODES:
        return family_code
    code = str(payload.get("code", "")).strip()
    if code in _HORIZON_FAMILY_CODES:
        return code
    if code in _HORIZON_CODE_LIBRARY:
        return str(_HORIZON_CODE_LIBRARY[code]["family_code"])
    inferred = infer_horizon_code_from_period(str(payload.get("label", "")).strip())
    if inferred in _HORIZON_FAMILY_CODES:
        return str(inferred)
    return default


def _specialize_horizon(
    horizon: Mapping[str, str],
    *,
    code: str,
    style: Optional[str] = None,
    fit_reason: Optional[str] = None,
    misfit_reason: Optional[str] = None,
) -> Dict[str, str]:
    result = dict(horizon)
    meta = dict(_HORIZON_CODE_LIBRARY.get(code) or {})
    if meta:
        result["code"] = code
        result["family_code"] = meta.get("family_code", result.get("family_code", "watch"))
        result["setup_code"] = meta.get("setup_code", "")
        result["setup_label"] = meta.get("setup_label", "")
    if style is not None:
        result["style"] = style
    if fit_reason is not None:
        result["fit_reason"] = fit_reason
    if misfit_reason is not None:
        result["misfit_reason"] = misfit_reason
    return _attach_expression_fields(result)


def _append_sentence(base: str, extra: str) -> str:
    head = str(base).strip()
    tail = str(extra).strip()
    if not tail:
        return head
    if not head:
        return tail
    if head.endswith(("。", "！", "？")):
        return f"{head}{tail}"
    return f"{head} {tail}"


def _is_crowded_mainline_consolidation(
    *,
    trade_state: str,
    technical_score: int,
    catalyst_score: int,
    relative_score: int,
    price_percentile_1y: float | None = None,
    rsi: float | None = None,
    sentiment_index: float | None = None,
    false_break_kind: str = "",
    divergence_signal: str = "",
    near_pressure: bool = False,
    phase_label: str = "",
) -> bool:
    high_position = any(
        condition
        for condition in (
            price_percentile_1y is not None and float(price_percentile_1y) >= 0.85,
            rsi is not None and float(rsi) >= 70.0,
            sentiment_index is not None and float(sentiment_index) >= 80.0,
        )
    )
    if not high_position:
        return False
    still_mainline_like = (
        relative_score >= 40
        or catalyst_score >= 35
        or "持有优于追高" in trade_state
        or "强势整理" in str(phase_label)
        or "上行中的整理" in str(phase_label)
        or (
            price_percentile_1y is not None
            and float(price_percentile_1y) >= 0.95
            and sentiment_index is not None
            and float(sentiment_index) >= 85.0
        )
    )
    if not still_mainline_like:
        return False
    distribution_signal = (
        false_break_kind == "bullish_false_break"
        or divergence_signal == "bearish"
        or near_pressure
        or technical_score < 45
    )
    return bool(distribution_signal)


def _with_execution_views(
    horizon: Mapping[str, str],
    *,
    asset_type: str,
    trade_state: str,
    technical_score: int,
    fundamental_score: int,
    catalyst_score: int,
    relative_score: int,
    risk_score: int,
    macro_reverse: bool,
) -> Dict[str, str]:
    result = dict(horizon)
    if asset_type not in {"cn_etf", "cn_fund"}:
        return result

    family_code = horizon_family_code(result)
    if family_code in {"long_term_allocation", "position_trade"}:
        allocation_view = "如果按配置视角理解，更适合作为一段主题/风格暴露的分批配置仓，接受中途震荡，但要持续复核主线、估值和容量。"
    elif family_code == "swing":
        allocation_view = "如果按配置视角理解，这条方向已经有一定配置价值，但更适合分批跟踪，不适合一次打满。"
    else:
        allocation_view = "如果按配置视角理解，这条方向还没到舒服的底仓阶段，先等趋势、赔率或催化再改善。"

    if family_code == "short_term":
        trading_view = "如果按交易视角理解，更看催化兑现、右侧确认和止损纪律，优先等回踩或放量确认，不追当天情绪。"
    elif family_code == "swing":
        trading_view = "如果按交易视角理解，更适合围绕回踩承接和几周级轮动去跟，不用把它当日内快进快出。"
    elif family_code in {"position_trade", "long_term_allocation"} and relative_score >= 60 and technical_score >= 45:
        trading_view = "如果按交易视角理解，更适合等强势整理或回踩确认后的再上车，不必因为短线波动频繁换仓。"
    else:
        trading_view = "如果按交易视角理解，当前更适合先等右侧确认，不把方向正确直接等同成今天就该追进去。"

    if macro_reverse:
        allocation_view = _append_sentence(allocation_view, "但当前宏观/风格逆风还在，仓位更适合保守。")
    elif fundamental_score >= 55 and risk_score >= 55:
        allocation_view = _append_sentence(allocation_view, "当前更适合把它当成中期方向载体，而不是只盯隔日波动。")

    if trade_state == "持有优于追高":
        trading_view = _append_sentence(trading_view, "当前位置更像持有或等回踩，不适合直接追高。")
    elif trade_state == "等右侧确认":
        trading_view = _append_sentence(trading_view, "先等趋势和资金重新同步，再把观察升级成执行。")
    elif catalyst_score >= 50 and technical_score < 45:
        trading_view = _append_sentence(trading_view, "催化已经先亮灯，但价格确认还没完全跟上。")

    result["allocation_view"] = allocation_view
    result["trading_view"] = trading_view
    return result


def _watch_contract_variant(
    *,
    asset_type: str,
    trade_state: str,
    technical_score: int,
    fundamental_score: int,
    catalyst_score: int,
    relative_score: int,
    risk_score: int,
    macro_reverse: bool,
    stop_hit_rate: float | None = None,
    win_rate_20d: float | None = None,
    confidence_score: int | None = None,
    price_percentile_1y: float | None = None,
    rsi: float | None = None,
    sentiment_index: float | None = None,
    false_break_kind: str = "",
    divergence_signal: str = "",
    near_pressure: bool = False,
    phase_label: str = "",
    source: str = "",
) -> Dict[str, str]:
    horizon = _base_contract("watch", source=source)

    if stop_hit_rate is not None and stop_hit_rate >= 0.6:
        return _specialize_horizon(
            horizon,
            code="watch_high_stopout_risk",
            style="这类信号当前更适合只留在观察名单里，先看风险释放和承接修复，不急着预设交易周期。",
            fit_reason="历史相似场景里的止损触发率偏高，先观察比提前试错更重要。",
            misfit_reason="不适合把它当成反弹试错仓去抢，尤其不适合在没有明确止损纪律时提前下手。",
        )

    if _is_crowded_mainline_consolidation(
        trade_state=trade_state,
        technical_score=technical_score,
        catalyst_score=catalyst_score,
        relative_score=relative_score,
        price_percentile_1y=price_percentile_1y,
        rsi=rsi,
        sentiment_index=sentiment_index,
        false_break_kind=false_break_kind,
        divergence_signal=divergence_signal,
        near_pressure=near_pressure,
        phase_label=phase_label,
    ):
        return _specialize_horizon(
            horizon,
            code="watch_crowded_mainline_consolidation",
            style="当前更像高位拥挤主线里的分歧消化，方向未必坏，但位置和节奏都不适合按修复早期理解。",
            fit_reason="主线和相对强弱未必坏，但高位分歧、假突破或情绪过热还在，先等拥挤消化后的再确认更稳。",
            misfit_reason="不适合把它当成低位右侧修复去抢，也不适合在情绪过热和压力位附近直接追价。",
        )

    if win_rate_20d is not None and win_rate_20d >= 0.65 and technical_score < 45:
        return _specialize_horizon(
            horizon,
            code="watch_historical_edge_without_confirmation",
            style="这更像历史样本不差、但眼前技术确认还没补齐的观察阶段，先等右侧信号比抢跑更稳。",
            fit_reason="历史样本并不差，但价格和动量确认还没补齐，当前更适合先等右侧信号。",
            misfit_reason="不适合把历史胜率直接等同成今天就能下手，更不适合在弱技术结构里提前抢跑。",
        )

    if fundamental_score <= 20:
        return _specialize_horizon(
            horizon,
            code="watch_fundamental_gap",
            style="当前更像基本面和赔率都偏弱的防守观察期，优先看风险是否继续出清，而不是先定义持有周期。",
            fit_reason="基本面支撑还没站住，先观察财务和景气能否修复，比急着给动作更重要。",
            misfit_reason="不适合按长线配置仓去理解，也不适合用“跌多了会反弹”替代真正确认。",
        )

    if fundamental_score >= 70 and relative_score < 40:
        return _specialize_horizon(
            horizon,
            code="watch_rotation_lag",
            style="更像基本面没有坏，但资金和轮动还没回来的观察阶段，先等强弱修复。",
            fit_reason="基本面还站得住，但轮动和承接没有同步回来，观察比抢反弹更重要。",
            misfit_reason="不适合把它当成趋势已经重启去理解，也不适合只凭基本面就提前上仓位。",
        )

    if catalyst_score >= 50 and technical_score < 40:
        return _specialize_horizon(
            horizon,
            code="watch_catalyst_without_price_confirmation",
            style="当前更像催化先亮灯、价格确认还没跟上的观察阶段，先看催化能否转成趋势。",
            fit_reason="催化有苗头，但价格和量能确认没跟上，先观察比只押单条消息更稳妥。",
            misfit_reason="不适合只看一条催化就提前下注，催化没转成价格确认前更像线索而不是动作。",
        )

    if technical_score < 35:
        return _specialize_horizon(
            horizon,
            code="watch_bottom_repair",
            style="当前更像下行后的底部修复，先等 MA20、MACD 这类确认信号，比抢反弹更重要。",
            fit_reason="技术结构还在底部修复阶段，先等趋势确认比仓促出手更重要。",
            misfit_reason="不适合把它当成右侧确认已经成立去理解，也不适合按超短抢反弹的思路硬做。",
        )

    if risk_score >= 70 and technical_score >= 35 and catalyst_score < 20:
        return _specialize_horizon(
            horizon,
            code="watch_defensive_without_catalyst",
            style="当前更像防守型观察阶段，风险收益比不算差，但催化和主线推进还没补齐。",
            fit_reason="波动和防守属性还在，但催化不足以支撑今天直接升级成正式动作。",
            misfit_reason="不适合把它当成进攻型机会去理解，也不适合在缺催化时提前放大仓位。",
        )

    if catalyst_score < 20 and technical_score >= 35:
        return _specialize_horizon(
            horizon,
            code="watch_structure_without_new_catalyst",
            style="当前更像结构没完全走坏、但催化和资金确认仍偏弱的观察阶段，先等新触发点。",
            fit_reason="价格结构不算最差，但催化和确认都偏弱，继续观察比仓促下手更稳。",
            misfit_reason="不适合把它当成高确定性右侧机会，也不适合在缺新催化时直接追进去。",
        )

    if risk_score < 35:
        return _specialize_horizon(
            horizon,
            code="watch_poor_reward_risk",
            style="当前更像风险收益比和波动窗口都不占优的观察阶段，先看风险是否重新变得可控。",
            fit_reason="风险收益比没有站到有利一侧，先保守观察比急着给动作更合适。",
            misfit_reason="不适合在风控边界还没清楚时先下手，也不适合用放大仓位去硬赌赔率修复。",
        )

    if macro_reverse and asset_type in {"cn_stock", "hk", "us"}:
        return _specialize_horizon(
            horizon,
            code="watch_macro_headwind",
            style="当前更像方向本身未必坏，但宏观和风格逆风还在的观察阶段，先等环境改善。",
            fit_reason="主线和宏观顺风还没形成共振，继续观察比提前定义进攻周期更稳妥。",
            misfit_reason="不适合把它当成环境已经顺风的进攻仓，也不适合忽视风格逆风去提前提杠杆。",
        )

    if confidence_score is not None and confidence_score < 40:
        return _specialize_horizon(
            horizon,
            code="watch_low_sample_confidence",
            style="当前更像线索级观察，先等更多确认信号补齐，再决定是否进入正式动作。",
            fit_reason="可参考的历史样本本身也不够扎实，先观察比提前上动作更稳。",
            misfit_reason="不适合把它当高确定性机会去理解，也不适合在低样本置信度下主动放大仓位。",
        )

    return horizon


def _swing_contract_variant(
    *,
    trade_state: str,
    technical_score: int,
    fundamental_score: int,
    catalyst_score: int,
    relative_score: int,
    risk_score: int,
    price_percentile_1y: float | None = None,
    rsi: float | None = None,
    sentiment_index: float | None = None,
    false_break_kind: str = "",
    divergence_signal: str = "",
    near_pressure: bool = False,
    phase_label: str = "",
    source: str = "",
) -> Dict[str, str]:
    horizon = _base_contract("swing", source=source)

    if _is_crowded_mainline_consolidation(
        trade_state=trade_state,
        technical_score=technical_score,
        catalyst_score=catalyst_score,
        relative_score=relative_score,
        price_percentile_1y=price_percentile_1y,
        rsi=rsi,
        sentiment_index=sentiment_index,
        false_break_kind=false_break_kind,
        divergence_signal=divergence_signal,
        near_pressure=near_pressure,
        phase_label=phase_label,
    ):
        return _specialize_horizon(
            horizon,
            code="swing_crowded_mainline_followthrough",
            style="当前更像高位主线里的强势整理，优势还在未来几周的延续，但位置已经不再舒服。",
            fit_reason="主线、轮动和相对强弱仍在，但当前位置更适合等分歧消化后的再确认，而不是把它当低位启动。",
            misfit_reason="不适合把波段跟踪误写成修复初期，也不适合在拥挤和压力位附近直接扩大仓位。",
        )

    if catalyst_score >= 60 and relative_score >= 65:
        return _specialize_horizon(
            horizon,
            code="swing_mainline_rotation_followthrough",
            style="当前更像主线轮动和催化共振驱动的波段跟踪，核心在未来几周能否继续得到资金确认。",
            fit_reason="催化和相对强弱都在线，优势主要集中在未来几周的轮动延续，而不是长周期兑现。",
            misfit_reason="不适合把短期热度直接等同成长线胜率，也不适合在情绪加速段盲目追价。",
        )

    if fundamental_score >= 80 and risk_score < 40:
        return _specialize_horizon(
            horizon,
            code="swing_fundamental_resilience_with_volatility",
            style="更像基本面先站住、但波动和风控约束仍偏紧的几周级跟踪，适合分批确认，不适合一次打满。",
            fit_reason="基本面和主线没有坏，但回撤和波动压力仍在，当前更适合按几周级别分批确认。",
            misfit_reason="不适合直接把它升格成长期底仓，也不适合在高波动阶段一次性重仓。",
        )

    if risk_score >= 50 and technical_score < 35:
        return _specialize_horizon(
            horizon,
            code="swing_repair_after_drawdown",
            style="更像低波或回撤修复框架里的波段跟踪，先看结构修复能否补齐，再决定是否提到执行层。",
            fit_reason="风险收益比相对不差，但技术确认仍偏弱，适合按修复节奏而不是单日强弱去跟。",
            misfit_reason="不适合按趋势已经重启去理解，也不适合忽视确认过程提前扩大仓位。",
        )

    if technical_score >= 55 and relative_score >= 55:
        return _specialize_horizon(
            horizon,
            code="swing_right_side_confirmation",
            style="当前更像右侧确认刚建立的波段跟踪，适合盯回踩承接，而不是把它当成长期持有逻辑。",
            fit_reason="价格和相对强弱已经给出一定确认，优势主要在未来几周顺势跟踪。",
            misfit_reason="不适合直接切换成长线底仓，也不适合因为单次回踩就完全按超短思路处理。",
        )

    return horizon


def _short_term_contract_variant(
    *,
    trade_state: str,
    technical_score: int,
    catalyst_score: int,
    relative_score: int,
    price_percentile_1y: float | None = None,
    rsi: float | None = None,
    sentiment_index: float | None = None,
    false_break_kind: str = "",
    divergence_signal: str = "",
    near_pressure: bool = False,
    phase_label: str = "",
    source: str = "",
) -> Dict[str, str]:
    horizon = _base_contract("short_term", source=source)
    if _is_crowded_mainline_consolidation(
        trade_state=trade_state,
        technical_score=technical_score,
        catalyst_score=catalyst_score,
        relative_score=relative_score,
        price_percentile_1y=price_percentile_1y,
        rsi=rsi,
        sentiment_index=sentiment_index,
        false_break_kind=false_break_kind,
        divergence_signal=divergence_signal,
        near_pressure=near_pressure,
        phase_label=phase_label,
    ):
        return _specialize_horizon(
            horizon,
            code="short_term_overbought_followthrough",
            style="当前更像强势主线里的短线跟随，优势仍在节奏，但位置已经偏热。",
            fit_reason="催化、强弱和短线节奏仍在，优势集中在顺势执行纪律，而不是把它当成舒服买点。",
            misfit_reason="不适合把短线强势等同成低位启动，也不适合忽视过热和拥挤直接追价。",
        )
    if catalyst_score >= 80 and technical_score >= 65:
        return _specialize_horizon(
            horizon,
            code="short_term_catalyst_breakout",
            style="当前更像催化驱动的短线突破，核心在事件兑现和右侧确认的连续性。",
            fit_reason="催化、动量和强弱已经形成共振，优势主要集中在接下来几个交易日的顺势执行。",
            misfit_reason="不适合把事件驱动的短线强势直接理解成长周期持有逻辑。",
        )
    return horizon


def _position_trade_contract_variant(
    *,
    trade_state: str,
    technical_score: int,
    fundamental_score: int,
    risk_score: int,
    source: str = "",
) -> Dict[str, str]:
    horizon = _base_contract("position_trade", source=source)
    if "持有优于追高" in trade_state:
        return _specialize_horizon(
            horizon,
            code="position_trade_hold_not_chase",
            style="更像中线方向没有坏、但当前位置更适合持有或等回踩，不适合急着追高的配置阶段。",
            fit_reason="中线逻辑和方向暴露还在，但操作上更适合分批承接，而不是因为主线正确就直接追进去。",
            misfit_reason="不适合把中线配置误写成短线加速段，也不适合忽视回踩和仓位节奏一次打满。",
        )
    if fundamental_score >= 85 and risk_score >= 70 and technical_score >= 55:
        return _specialize_horizon(
            horizon,
            code="position_trade_core_accumulation",
            style="更像一段完整主线里的中线分批配置，允许中途波动，但不靠隔日涨跌定义成败。",
            fit_reason="基本面、趋势和风险收益都不差，更适合按几个月维度做分批配置。",
            misfit_reason="不适合退回成纯超短交易，也不适合因为一两天震荡就破坏中线计划。",
        )
    return horizon


def _long_term_contract_variant(
    *,
    technical_score: int,
    fundamental_score: int,
    risk_score: int,
    source: str = "",
) -> Dict[str, str]:
    horizon = _base_contract("long_term_allocation", source=source)
    if fundamental_score >= 85 and risk_score >= 70 and technical_score >= 55:
        return _specialize_horizon(
            horizon,
            code="long_term_quality_compounding",
            style="更适合作为质量复利型的中长期底仓来跟踪，允许中途波动，但要持续复核主线和基本面。",
            fit_reason="基本面、风险收益和中期趋势都更完整，持有逻辑不只依赖眼前一两周的催化。",
            misfit_reason="不适合按纯短线追涨杀跌来处理，也不适合长期不复核就机械持有。",
        )
    return horizon


def infer_horizon_code_from_period(period_text: str) -> Optional[str]:
    text = str(period_text or "").strip().lower()
    if not text:
        return None
    if any(token in text for token in ("观察", "等待", "更好窗口", "暂不")):
        return "watch"
    if any(token in text for token in ("长线", "长期", "一年", "12月", "6-12月", "6个月", "年")):
        return "long_term_allocation"
    if any(token in text for token in ("中线", "1-3月", "2-3月", "3个月")):
        return "position_trade"
    if "波段" in text:
        return "swing"
    if any(token in text for token in ("短线", "1-2周", "3-10日", "3-5日", "超短")):
        return "short_term"
    week_match = re.search(r"(\d+)\s*-\s*(\d+)\s*周", text)
    if week_match:
        high = int(week_match.group(2))
        return "short_term" if high <= 2 else "swing"
    if "周" in text:
        return "swing"
    month_match = re.search(r"(\d+)\s*-\s*(\d+)\s*月", text)
    if month_match:
        high = int(month_match.group(2))
        return "position_trade" if high <= 3 else "long_term_allocation"
    return None


def get_horizon_contract(code: str, *, source: str = "") -> Dict[str, str]:
    normalized = str(code or "").strip()
    if normalized in _HORIZON_FAMILY_CODES:
        return _base_contract(normalized, source=source)
    if normalized in _HORIZON_CODE_LIBRARY:
        family_code = str(_HORIZON_CODE_LIBRARY[normalized]["family_code"])
        return _specialize_horizon(_base_contract(family_code, source=source), code=normalized)
    return _base_contract("watch", source=source)


def build_analysis_horizon_profile(
    *,
    rating: int,
    asset_type: str,
    technical_score: int,
    fundamental_score: int,
    catalyst_score: int,
    relative_score: int,
    risk_score: int,
    macro_reverse: bool,
    trade_state: str,
    direction: str,
    position: str,
    stop_hit_rate: float | None = None,
    win_rate_20d: float | None = None,
    confidence_score: int | None = None,
    price_percentile_1y: float | None = None,
    rsi: float | None = None,
    sentiment_index: float | None = None,
    false_break_kind: str = "",
    divergence_signal: str = "",
    near_pressure: bool = False,
    phase_label: str = "",
) -> Dict[str, str]:
    trade_state = str(trade_state).strip()
    direction = str(direction).strip()
    position = str(position).strip()

    if direction in {"回避", "观望"} and ("暂不出手" in position or rating <= 1):
        return _with_execution_views(
            _watch_contract_variant(
                asset_type=asset_type,
                trade_state=trade_state,
                technical_score=technical_score,
                fundamental_score=fundamental_score,
                catalyst_score=catalyst_score,
                relative_score=relative_score,
                risk_score=risk_score,
                macro_reverse=macro_reverse,
                stop_hit_rate=stop_hit_rate,
                win_rate_20d=win_rate_20d,
                confidence_score=confidence_score,
                price_percentile_1y=price_percentile_1y,
                rsi=rsi,
                sentiment_index=sentiment_index,
                false_break_kind=false_break_kind,
                divergence_signal=divergence_signal,
                near_pressure=near_pressure,
                phase_label=phase_label,
                source="analysis_inferred",
            ),
            asset_type=asset_type,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
            trade_state=trade_state,
        )

    if rating >= 4 and fundamental_score >= 70 and risk_score >= 65 and not macro_reverse:
        return _with_execution_views(
            _long_term_contract_variant(
                technical_score=technical_score,
                fundamental_score=fundamental_score,
                risk_score=risk_score,
                source="analysis_inferred",
            ),
            asset_type=asset_type,
            trade_state=trade_state,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
        )

    if rating >= 3 and fundamental_score >= 58 and risk_score >= 55 and not macro_reverse:
        return _with_execution_views(
            _position_trade_contract_variant(
                trade_state=trade_state,
                technical_score=technical_score,
                fundamental_score=fundamental_score,
                risk_score=risk_score,
                source="analysis_inferred",
            ),
            asset_type=asset_type,
            trade_state=trade_state,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
        )

    if catalyst_score >= 65 and technical_score >= 55 and relative_score >= 60:
        return _with_execution_views(
            _short_term_contract_variant(
                trade_state=trade_state,
                technical_score=technical_score,
                catalyst_score=catalyst_score,
                relative_score=relative_score,
                price_percentile_1y=price_percentile_1y,
                rsi=rsi,
                sentiment_index=sentiment_index,
                false_break_kind=false_break_kind,
                divergence_signal=divergence_signal,
                near_pressure=near_pressure,
                phase_label=phase_label,
                source="analysis_inferred",
            ),
            asset_type=asset_type,
            trade_state=trade_state,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
        )

    if rating >= 2 or (risk_score >= 70 and relative_score >= 60) or (technical_score >= 55 and relative_score >= 55):
        return _with_execution_views(
            _swing_contract_variant(
                trade_state=trade_state,
                technical_score=technical_score,
                fundamental_score=fundamental_score,
                catalyst_score=catalyst_score,
                relative_score=relative_score,
                risk_score=risk_score,
                price_percentile_1y=price_percentile_1y,
                rsi=rsi,
                sentiment_index=sentiment_index,
                false_break_kind=false_break_kind,
                divergence_signal=divergence_signal,
                near_pressure=near_pressure,
                phase_label=phase_label,
                source="analysis_inferred",
            ),
            asset_type=asset_type,
            trade_state=trade_state,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
        )

    if "持有优于追高" in trade_state and asset_type in {"cn_etf", "cn_fund"} and fundamental_score >= 50 and risk_score >= 50:
        return _with_execution_views(
            _position_trade_contract_variant(
                trade_state=trade_state,
                technical_score=technical_score,
                fundamental_score=fundamental_score,
                risk_score=risk_score,
                source="analysis_inferred",
            ),
            asset_type=asset_type,
            trade_state=trade_state,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
        )

    return _with_execution_views(
        _watch_contract_variant(
            asset_type=asset_type,
            trade_state=trade_state,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
            stop_hit_rate=stop_hit_rate,
            win_rate_20d=win_rate_20d,
            confidence_score=confidence_score,
            price_percentile_1y=price_percentile_1y,
            rsi=rsi,
            sentiment_index=sentiment_index,
            false_break_kind=false_break_kind,
            divergence_signal=divergence_signal,
            near_pressure=near_pressure,
            phase_label=phase_label,
            source="analysis_inferred",
        ),
        asset_type=asset_type,
        trade_state=trade_state,
        technical_score=technical_score,
        fundamental_score=fundamental_score,
        catalyst_score=catalyst_score,
        relative_score=relative_score,
        risk_score=risk_score,
        macro_reverse=macro_reverse,
    )


def build_trade_plan_horizon(
    *,
    thesis: Mapping[str, Any] | None,
    action: str,
    projected_weight: float,
    suggested_max_weight: float,
    execution: Mapping[str, Any] | None = None,
    signal_snapshot: Mapping[str, Any] | None = None,
) -> Dict[str, str]:
    thesis_payload = dict(thesis or {})
    signal_payload = dict(signal_snapshot or {})
    execution_payload = dict(execution or {})
    action = str(action or "").strip().lower()

    period_text = str(
        thesis_payload.get("holding_period")
        or thesis_payload.get("period")
        or thesis_payload.get("timeframe")
        or ""
    ).strip()
    code = infer_horizon_code_from_period(period_text)
    if code:
        horizon = _base_contract(code, source="thesis_stated")
        thesis_code = {
            "watch": "watch_thesis_stated",
            "short_term": "short_term_thesis_stated",
            "swing": "swing_thesis_stated",
            "position_trade": "position_trade_thesis_stated",
            "long_term_allocation": "long_term_thesis_stated",
        }.get(code)
        if thesis_code:
            horizon = _specialize_horizon(horizon, code=thesis_code)
        horizon["fit_reason"] = f"原始 thesis 的预期周期写的是 `{period_text}`，当前更适合按 `{horizon['label']}` 的框架理解。"
    else:
        ma_signal = str(signal_payload.get("ma_signal", "") or "").lower()
        macd_signal = str(signal_payload.get("macd_signal", "") or "").lower()
        rsi = signal_payload.get("rsi")
        return_20d = signal_payload.get("return_20d")
        tradability = str(execution_payload.get("tradability_label", "") or "")
        if action == "sell":
            horizon = _base_contract("watch", source="trade_plan_inferred")
            horizon = _specialize_horizon(horizon, code="watch_risk_disposal")
            horizon["fit_reason"] = "这次问题更偏减仓或风险处置，不是在定义新的进攻周期。"
            horizon["misfit_reason"] = "卖出预演不适合直接套用长线建仓或短线追价的逻辑。"
        elif tradability in {"顺畅", "可成交"} and (ma_signal == "bullish" or macd_signal == "bullish" or (return_20d is not None and float(return_20d) > 0.05)):
            horizon = _base_contract("swing", source="trade_plan_inferred")
        elif (ma_signal == "bullish" or (return_20d is not None and float(return_20d) > 0.08)) and rsi is not None and float(rsi) >= 65:
            horizon = _base_contract("short_term", source="trade_plan_inferred")
        else:
            horizon = _base_contract("watch", source="trade_plan_inferred")

    if projected_weight > suggested_max_weight + 1e-9:
        horizon["misfit_reason"] = _append_sentence(
            horizon["misfit_reason"],
            "当前预演仓位已高于更合理上限，更不适合一次打满。",
        )
    tradability = str(execution_payload.get("tradability_label", "") or "")
    if tradability in {"谨慎", "冲击偏高", "数据不足"}:
        horizon["misfit_reason"] = _append_sentence(
            horizon["misfit_reason"],
            f"当前可成交性偏 `{tradability}`，更适合分批或继续等窗口。",
        )
    return horizon


def build_review_horizon(
    *,
    thesis: Mapping[str, Any] | None,
    signal_snapshot: Mapping[str, Any] | None,
    action: str,
    signal_alignment: str,
    decision_snapshot: Mapping[str, Any] | None = None,
) -> Dict[str, str]:
    decision_payload = dict(decision_snapshot or {})
    historical = dict(decision_payload.get("horizon") or {})
    if historical:
        if not historical.get("source"):
            historical["source"] = "historical_snapshot"
        return historical

    thesis_payload = dict(thesis or {})
    signal_payload = dict(signal_snapshot or {})
    period_text = str(
        thesis_payload.get("holding_period")
        or thesis_payload.get("period")
        or thesis_payload.get("timeframe")
        or ""
    ).strip()
    code = infer_horizon_code_from_period(period_text)
    if code:
        horizon = _base_contract(code, source="review_reconstructed_from_thesis")
        thesis_code = {
            "watch": "watch_thesis_stated",
            "short_term": "short_term_thesis_stated",
            "swing": "swing_thesis_stated",
            "position_trade": "position_trade_thesis_stated",
            "long_term_allocation": "long_term_thesis_stated",
        }.get(code)
        if thesis_code:
            horizon = _specialize_horizon(horizon, code=thesis_code)
        horizon["fit_reason"] = f"历史 thesis 里写的预期周期是 `{period_text}`，这笔交易更应按 `{horizon['label']}` 的框架复盘。"
    else:
        ma_signal = str(signal_payload.get("ma_signal", "") or "").lower()
        macd_signal = str(signal_payload.get("macd_signal", "") or "").lower()
        if str(action or "").lower() == "sell":
            horizon = _base_contract("watch", source="review_reconstructed_from_signal")
            horizon = _specialize_horizon(horizon, code="watch_risk_disposal")
            horizon["fit_reason"] = "这笔动作更偏减仓/防守，不是在定义新的进攻持有周期。"
            horizon["misfit_reason"] = "不适合把减仓动作直接复盘成长线建仓或短线追涨。"
        elif "顺势" in str(signal_alignment) and ma_signal == "bullish" and macd_signal == "bullish":
            horizon = _base_contract("swing", source="review_reconstructed_from_signal")
        else:
            horizon = _base_contract("watch", source="review_reconstructed_from_signal")

    if "逆势" in str(signal_alignment):
        horizon["misfit_reason"] = _append_sentence(horizon["misfit_reason"], "当时属于逆势动作，执行门槛本来就应该更高。")
    return horizon
