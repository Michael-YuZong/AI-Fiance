"""Shared holding-horizon contracts across analysis, trade planning, and review."""

from __future__ import annotations

import re
from typing import Any, Dict, Mapping, Optional


def _contract(
    code: str,
    label: str,
    style: str,
    fit_reason: str,
    misfit_reason: str,
    *,
    source: str = "",
) -> Dict[str, str]:
    return {
        "code": code,
        "label": label,
        "style": style,
        "fit_reason": fit_reason,
        "misfit_reason": misfit_reason,
        "source": source,
    }


def _base_contract(code: str, *, source: str = "") -> Dict[str, str]:
    library = {
        "watch": _contract(
            "watch",
            "观察期",
            "先等催化、趋势或风险收益比进一步确认，不急着把它定义成短线执行仓或长线配置仓。",
            "当前信号还没共振到足以支撑正式动作，继续观察比仓促出手更重要。",
            "现在不适合直接按短线执行仓或长线配置仓去理解。",
            source=source,
        ),
        "short_term": _contract(
            "short_term",
            "短线交易（3-10日）",
            "更看催化、趋势和执行节奏，适合盯右侧确认和止损，不适合当成长线底仓。",
            "当前更强的是催化、相对强弱和执行节奏，优势主要集中在接下来几个交易日到一两周内。",
            "现在不适合直接当成长线底仓，一旦催化和强势股状态失效要更快处理。",
            source=source,
        ),
        "swing": _contract(
            "swing",
            "波段跟踪（2-6周）",
            "更适合按几周级别的波段节奏去跟踪，等确认和回踩，不靠单日冲动去追。",
            "趋势、轮动或风险收益比已经有基础，但更依赖未来几周节奏，而不是长周期基本面完全兑现。",
            "现在不适合把它当长期底仓，也不适合只按隔夜消息去赌超短。",
            source=source,
        ),
        "position_trade": _contract(
            "position_trade",
            "中线配置（1-3月）",
            "更像 1-3 个月的分批配置或波段跟踪，不按隔日涨跌去做快进快出。",
            "基本面、风险收益和趋势至少有两项站得住，更适合按一段完整主线去拿，而不是只博短催化。",
            "现在不适合当成纯隔夜交易，也还没强到可以长期不复核地持有一年以上。",
            source=source,
        ),
        "long_term_allocation": _contract(
            "long_term_allocation",
            "长线配置（6-12月）",
            "更适合作为中长期底仓来跟踪，允许短线波动，但要持续复核主线、基本面和风险预算。",
            "基本面、风险收益和主线顺风更完整，持有逻辑不只依赖眼前一两周的催化。",
            "现在不适合按纯短线追价来理解，短线节奏错了也不能破坏长线仓位纪律。",
            source=source,
        ),
    }
    return dict(library[code])


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


def _watch_contract_variant(
    *,
    asset_type: str,
    technical_score: int,
    fundamental_score: int,
    catalyst_score: int,
    relative_score: int,
    risk_score: int,
    macro_reverse: bool,
    stop_hit_rate: float | None = None,
    win_rate_20d: float | None = None,
    confidence_score: int | None = None,
    source: str = "",
) -> Dict[str, str]:
    horizon = _base_contract("watch", source=source)

    if stop_hit_rate is not None and stop_hit_rate >= 0.6:
        horizon["style"] = "这类信号当前更适合只留在观察名单里，先看风险释放和承接修复，不急着预设交易周期。"
        horizon["fit_reason"] = "历史相似场景里的止损触发率偏高，先观察比提前试错更重要。"
        horizon["misfit_reason"] = "不适合把它当成反弹试错仓去抢，尤其不适合在没有明确止损纪律时提前下手。"
        return horizon

    if win_rate_20d is not None and win_rate_20d >= 0.65 and technical_score < 45:
        horizon["style"] = "这更像历史样本不差、但眼前技术确认还没补齐的观察阶段，先等右侧信号比抢跑更稳。"
        horizon["fit_reason"] = "历史样本并不差，但价格和动量确认还没补齐，当前更适合先等右侧信号。"
        horizon["misfit_reason"] = "不适合把历史胜率直接等同成今天就能下手，更不适合在弱技术结构里提前抢跑。"
        return horizon

    if fundamental_score <= 20:
        horizon["style"] = "当前更像基本面和赔率都偏弱的防守观察期，优先看风险是否继续出清，而不是先定义持有周期。"
        horizon["fit_reason"] = "基本面支撑还没站住，先观察财务和景气能否修复，比急着给动作更重要。"
        horizon["misfit_reason"] = "不适合按长线配置仓去理解，也不适合用“跌多了会反弹”替代真正确认。"
        return horizon

    if fundamental_score >= 70 and relative_score < 40:
        horizon["style"] = "更像基本面没有坏，但资金和轮动还没回来的观察阶段，先等强弱修复。"
        horizon["fit_reason"] = "基本面还站得住，但轮动和承接没有同步回来，观察比抢反弹更重要。"
        horizon["misfit_reason"] = "不适合把它当成趋势已经重启去理解，也不适合只凭基本面就提前上仓位。"
        return horizon

    if catalyst_score >= 50 and technical_score < 40:
        horizon["style"] = "当前更像催化先亮灯、价格确认还没跟上的观察阶段，先看催化能否转成趋势。"
        horizon["fit_reason"] = "催化有苗头，但价格和量能确认没跟上，先观察比只押单条消息更稳妥。"
        horizon["misfit_reason"] = "不适合只看一条催化就提前下注，催化没转成价格确认前更像线索而不是动作。"
        return horizon

    if technical_score < 35:
        horizon["style"] = "当前还处在弱趋势或修复早期，先等 MA20、MACD 这类确认信号，比抢反弹更重要。"
        horizon["fit_reason"] = "技术结构还在修复早期，先等趋势确认比仓促出手更重要。"
        horizon["misfit_reason"] = "不适合把它当成右侧确认已经成立去理解，也不适合按超短抢反弹的思路硬做。"
        return horizon

    if risk_score >= 70 and technical_score >= 35 and catalyst_score < 20:
        horizon["style"] = "当前更像防守型观察阶段，风险收益比不算差，但催化和主线推进还没补齐。"
        horizon["fit_reason"] = "波动和防守属性还在，但催化不足以支撑今天直接升级成正式动作。"
        horizon["misfit_reason"] = "不适合把它当成进攻型机会去理解，也不适合在缺催化时提前放大仓位。"
        return horizon

    if catalyst_score < 20 and technical_score >= 35:
        horizon["style"] = "当前更像结构没完全走坏、但催化和资金确认仍偏弱的观察阶段，先等新触发点。"
        horizon["fit_reason"] = "价格结构不算最差，但催化和确认都偏弱，继续观察比仓促下手更稳。"
        horizon["misfit_reason"] = "不适合把它当成高确定性右侧机会，也不适合在缺新催化时直接追进去。"
        return horizon

    if risk_score < 35:
        horizon["style"] = "当前更像风险收益比和波动窗口都不占优的观察阶段，先看风险是否重新变得可控。"
        horizon["fit_reason"] = "风险收益比没有站到有利一侧，先保守观察比急着给动作更合适。"
        horizon["misfit_reason"] = "不适合在风控边界还没清楚时先下手，也不适合用放大仓位去硬赌赔率修复。"
        return horizon

    if macro_reverse and asset_type in {"cn_stock", "hk", "us"}:
        horizon["style"] = "当前更像方向本身未必坏，但宏观和风格逆风还在的观察阶段，先等环境改善。"
        horizon["fit_reason"] = "主线和宏观顺风还没形成共振，继续观察比提前定义进攻周期更稳妥。"
        horizon["misfit_reason"] = "不适合把它当成环境已经顺风的进攻仓，也不适合忽视风格逆风去提前提杠杆。"
        return horizon

    if confidence_score is not None and confidence_score < 40:
        horizon["style"] = "当前更像线索级观察，先等更多确认信号补齐，再决定是否进入正式动作。"
        horizon["fit_reason"] = "可参考的历史样本本身也不够扎实，先观察比提前上动作更稳。"
        horizon["misfit_reason"] = "不适合把它当高确定性机会去理解，也不适合在低样本置信度下主动放大仓位。"
        return horizon

    return horizon


def _swing_contract_variant(
    *,
    technical_score: int,
    fundamental_score: int,
    catalyst_score: int,
    relative_score: int,
    risk_score: int,
    source: str = "",
) -> Dict[str, str]:
    horizon = _base_contract("swing", source=source)

    if catalyst_score >= 60 and relative_score >= 65:
        horizon["style"] = "当前更像主线轮动和催化共振驱动的波段跟踪，核心在未来几周能否继续得到资金确认。"
        horizon["fit_reason"] = "催化和相对强弱都在线，优势主要集中在未来几周的轮动延续，而不是长周期兑现。"
        horizon["misfit_reason"] = "不适合把短期热度直接等同成长线胜率，也不适合在情绪加速段盲目追价。"
        return horizon

    if fundamental_score >= 80 and risk_score < 40:
        horizon["style"] = "更像基本面先站住、但波动和风控约束仍偏紧的几周级跟踪，适合分批确认，不适合一次打满。"
        horizon["fit_reason"] = "基本面和主线没有坏，但回撤和波动压力仍在，当前更适合按几周级别分批确认。"
        horizon["misfit_reason"] = "不适合直接把它升格成长期底仓，也不适合在高波动阶段一次性重仓。"
        return horizon

    if risk_score >= 50 and technical_score < 35:
        horizon["style"] = "更像低波或回撤修复框架里的波段跟踪，先看结构修复能否补齐，再决定是否提到执行层。"
        horizon["fit_reason"] = "风险收益比相对不差，但技术确认仍偏弱，适合按修复节奏而不是单日强弱去跟。"
        horizon["misfit_reason"] = "不适合按趋势已经重启去理解，也不适合忽视确认过程提前扩大仓位。"
        return horizon

    if technical_score >= 55 and relative_score >= 55:
        horizon["style"] = "当前更像右侧确认刚建立的波段跟踪，适合盯回踩承接，而不是把它当成长期持有逻辑。"
        horizon["fit_reason"] = "价格和相对强弱已经给出一定确认，优势主要在未来几周顺势跟踪。"
        horizon["misfit_reason"] = "不适合直接切换成长线底仓，也不适合因为单次回踩就完全按超短思路处理。"
        return horizon

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
    if normalized not in {"watch", "short_term", "swing", "position_trade", "long_term_allocation"}:
        normalized = "watch"
    return _base_contract(normalized, source=source)


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
) -> Dict[str, str]:
    trade_state = str(trade_state).strip()
    direction = str(direction).strip()
    position = str(position).strip()

    if direction in {"回避", "观望"} and ("暂不出手" in position or rating <= 1):
        return _watch_contract_variant(
            asset_type=asset_type,
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            macro_reverse=macro_reverse,
            stop_hit_rate=stop_hit_rate,
            win_rate_20d=win_rate_20d,
            confidence_score=confidence_score,
            source="analysis_inferred",
        )

    if rating >= 4 and fundamental_score >= 70 and risk_score >= 65 and not macro_reverse:
        return _base_contract("long_term_allocation", source="analysis_inferred")

    if rating >= 3 and fundamental_score >= 58 and risk_score >= 55 and not macro_reverse:
        return _base_contract("position_trade", source="analysis_inferred")

    if catalyst_score >= 65 and technical_score >= 55 and relative_score >= 60:
        return _base_contract("short_term", source="analysis_inferred")

    if rating >= 2 or (risk_score >= 70 and relative_score >= 60) or (technical_score >= 55 and relative_score >= 55):
        return _swing_contract_variant(
            technical_score=technical_score,
            fundamental_score=fundamental_score,
            catalyst_score=catalyst_score,
            relative_score=relative_score,
            risk_score=risk_score,
            source="analysis_inferred",
        )

    if "持有优于追高" in trade_state and asset_type in {"cn_etf", "cn_fund"} and fundamental_score >= 50 and risk_score >= 50:
        return _base_contract("position_trade", source="analysis_inferred")

    return _watch_contract_variant(
        asset_type=asset_type,
        technical_score=technical_score,
        fundamental_score=fundamental_score,
        catalyst_score=catalyst_score,
        relative_score=relative_score,
        risk_score=risk_score,
        macro_reverse=macro_reverse,
        stop_hit_rate=stop_hit_rate,
        win_rate_20d=win_rate_20d,
        confidence_score=confidence_score,
        source="analysis_inferred",
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
        horizon["fit_reason"] = f"原始 thesis 的预期周期写的是 `{period_text}`，当前更适合按 `{horizon['label']}` 的框架理解。"
    else:
        ma_signal = str(signal_payload.get("ma_signal", "") or "").lower()
        macd_signal = str(signal_payload.get("macd_signal", "") or "").lower()
        rsi = signal_payload.get("rsi")
        return_20d = signal_payload.get("return_20d")
        tradability = str(execution_payload.get("tradability_label", "") or "")
        if action == "sell":
            horizon = _base_contract("watch", source="trade_plan_inferred")
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
        horizon["fit_reason"] = f"历史 thesis 里写的预期周期是 `{period_text}`，这笔交易更应按 `{horizon['label']}` 的框架复盘。"
    else:
        ma_signal = str(signal_payload.get("ma_signal", "") or "").lower()
        macd_signal = str(signal_payload.get("macd_signal", "") or "").lower()
        if str(action or "").lower() == "sell":
            horizon = _base_contract("watch", source="review_reconstructed_from_signal")
            horizon["fit_reason"] = "这笔动作更偏减仓/防守，不是在定义新的进攻持有周期。"
            horizon["misfit_reason"] = "不适合把减仓动作直接复盘成长线建仓或短线追涨。"
        elif "顺势" in str(signal_alignment) and ma_signal == "bullish" and macd_signal == "bullish":
            horizon = _base_contract("swing", source="review_reconstructed_from_signal")
        else:
            horizon = _base_contract("watch", source="review_reconstructed_from_signal")

    if "逆势" in str(signal_alignment):
        horizon["misfit_reason"] = _append_sentence(horizon["misfit_reason"], "当时属于逆势动作，执行门槛本来就应该更高。")
    return horizon
