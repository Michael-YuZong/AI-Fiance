"""Thesis JSON storage."""

from __future__ import annotations

from pathlib import Path
from datetime import datetime
from typing import Any, Dict, List, Mapping

from src.utils.config import PROJECT_ROOT
from src.utils.data import load_json, save_json


_EVENT_DIGEST_FIELDS = (
    "contract_version",
    "status",
    "lead_layer",
    "lead_detail",
    "importance",
    "lead_title",
    "impact_summary",
    "thesis_scope",
    "importance_reason",
    "changed_what",
    "next_step",
)
_EVENT_DIGEST_STATUS_ORDER = {
    "待补充": 0,
    "待复核": 1,
    "已消化": 2,
}
_EVENT_LAYER_ORDER = {
    "新闻": 0,
    "行业主题事件": 1,
    "政策": 2,
    "公告": 3,
    "财报": 4,
}
_EVENT_IMPORTANCE_ORDER = {
    "low": 0,
    "medium": 1,
    "high": 2,
}
_EVENT_IMPORTANCE_LABELS = {
    "high": "高",
    "medium": "中",
    "low": "低",
}
_EVENT_DETAIL_FOCUS_HINTS = (
    ("价格/排产验证", "供需、价格和排产验证"),
    ("海外映射/链式催化", "海外事件外溢与本土映射"),
    ("情绪热度", "市场关注度和资金偏好"),
    ("毛利率/费用率改善", "利润率与经营杠杆"),
    ("现金流/合同负债改善", "回款质量与订单前瞻"),
    ("存货/减值压力", "去库与资产质量压力"),
    ("盈利/指引上修", "盈利与指引兑现"),
    ("盈利/指引承压", "盈利承压与需求回落"),
    ("资本开支/扩产", "产能投入与未来兑现"),
    ("回购/分红", "股东回报与估值支撑"),
    ("中标/订单", "订单与项目兑现"),
    ("产品/新品", "产品验证与放量兑现"),
    ("融资/定增", "资本结构与估值稀释"),
    ("并购/重组", "外延整合与资产注入"),
    ("减持/解禁", "筹码供给与承接压力"),
    ("财政支持/名单落地", "预算与名单兑现"),
    ("价格机制/收费调整", "价格传导与收益口径"),
    ("配套细则", "执行框架与覆盖范围"),
    ("方向表态", "政策预期与风险偏好"),
    ("直接执行", "执行主体与落地兑现"),
)
_NEGATIVE_EVENT_DETAIL_MARKERS = (
    "盈利/指引承压",
    "存货/减值压力",
    "减持/解禁",
)
_REVIEW_PRIORITY_LABELS = (
    (85, "高"),
    (60, "中"),
    (0, "低"),
)


def _default_review_queue_ledger() -> Dict[str, Any]:
    return {
        "contract_version": "thesis_review_queue.v1",
        "active": [],
        "history": {},
        "last_updated_at": "",
        "last_source": "",
        "last_transitions": {
            "new_entries": [],
            "resolved_entries": [],
            "stale_high_priority": [],
        },
    }


def _safe_text(value: Any) -> str:
    return str(value or "").strip()


def _compact_event_digest_snapshot(payload: Mapping[str, Any] | None) -> Dict[str, Any]:
    snapshot = dict(payload or {})
    if not snapshot:
        return {}
    compact: Dict[str, Any] = {}
    for key in _EVENT_DIGEST_FIELDS:
        if key == "contract_version":
            compact[key] = _safe_text(snapshot.get(key)) or "event_digest.v1"
            continue
        value = _safe_text(snapshot.get(key))
        if value:
            compact[key] = value
    if compact and not _safe_text(compact.get("contract_version")):
        compact["contract_version"] = "event_digest.v1"
    return compact


def _event_digest_signature(payload: Mapping[str, Any] | None) -> tuple[str, ...]:
    snapshot = _compact_event_digest_snapshot(payload)
    if not snapshot:
        return ()
    return tuple(_safe_text(snapshot.get(field)) for field in _EVENT_DIGEST_FIELDS)


def _event_layer_rank(layer: str) -> int:
    return _EVENT_LAYER_ORDER.get(_safe_text(layer), -1)


def _event_importance_rank(value: str) -> int:
    return _EVENT_IMPORTANCE_ORDER.get(_safe_text(value), -1)


def _event_importance_label(value: str) -> str:
    text = _safe_text(value)
    return _EVENT_IMPORTANCE_LABELS.get(text, text or "未定义")


def _event_detail_focus(detail: Any) -> str:
    text = _safe_text(detail)
    for marker, label in _EVENT_DETAIL_FOCUS_HINTS:
        if marker in text:
            return label
    return text


def _event_detail_is_negative(detail: Any) -> bool:
    text = _safe_text(detail)
    return any(marker in text for marker in _NEGATIVE_EVENT_DETAIL_MARKERS)


def _event_detail_transition_suffix(previous_detail: Any, current_detail: Any) -> str:
    previous_focus = _event_detail_focus(previous_detail)
    current_focus = _event_detail_focus(current_detail)
    if previous_focus and current_focus and previous_focus != current_focus:
        return f" 研究焦点也已从 `{previous_focus}` 切到 `{current_focus}`。"
    if current_focus:
        return f" 当前更该按 `{current_focus}` 这层复核。"
    return ""


def _detail_retyped_transition(previous_detail: str, current_detail: str) -> tuple[str, str, str] | None:
    if "情绪热度" in previous_detail and "价格/排产验证" in current_detail:
        return (
            "升级",
            "主题从热度切到景气验证",
            "主题线索已从情绪热度下沉到景气和价格验证，thesis 可以按更高确定性处理。",
        )
    if "海外映射/链式催化" in previous_detail and "价格/排产验证" in current_detail:
        return (
            "升级",
            "主题从映射切到景气验证",
            "这条主题已从海外映射阶段下沉到本土景气验证，原 thesis 可以按更直接的传导层复核。",
        )
    if "现金流/合同负债改善" in previous_detail and "毛利率/费用率改善" in current_detail:
        return (
            "升级",
            "财报从前瞻验证切到利润兑现",
            "财报焦点已从前瞻验证下沉到利润率兑现层，thesis 确定性可以同步上调。",
        )
    if _event_detail_is_negative(current_detail):
        return (
            "削弱",
            "研究焦点转向负面压力",
            "当前研究焦点已经转向更直接的负面压力层，原 thesis 需要先降级重审。",
        )
    return None


def _event_understanding_summary(payload: Mapping[str, Any] | None) -> str:
    snapshot = _compact_event_digest_snapshot(payload)
    if not snapshot:
        return ""
    detail = _safe_text(snapshot.get("lead_detail"))
    impact_summary = _safe_text(snapshot.get("impact_summary"))
    thesis_scope = _safe_text(snapshot.get("thesis_scope"))
    importance_reason = _safe_text(snapshot.get("importance_reason"))
    parts: List[str] = []
    if detail:
        parts.append(f"更该前置的是 `{detail}`")
    if impact_summary:
        parts.append(f"更直接影响 `{impact_summary}`")
    if thesis_scope:
        parts.append(f"先按 `{thesis_scope}` 处理")
    if importance_reason:
        parts.append(f"优先级判断是：{importance_reason}")
    return "，".join(parts)


def _append_understanding(summary: str, understanding: str, *, prefix: str = "当前") -> str:
    base = _safe_text(summary).rstrip("。")
    clause = _safe_text(understanding).rstrip("。")
    if not clause:
        return base
    if prefix:
        clause = f"{prefix}{clause}"
    if not base:
        return f"{clause}。"
    return f"{base}。 {clause}。"


def compare_event_digest_snapshots(
    previous: Mapping[str, Any] | None,
    current: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    previous_snapshot = _compact_event_digest_snapshot(previous)
    current_snapshot = _compact_event_digest_snapshot(current)
    if not current_snapshot:
        return {
            "changed": False,
            "change_type": "missing_current",
            "summary": "",
            "previous_snapshot": previous_snapshot,
            "current_snapshot": current_snapshot,
        }

    previous_status = _safe_text(previous_snapshot.get("status"))
    current_status = _safe_text(current_snapshot.get("status"))
    previous_layer = _safe_text(previous_snapshot.get("lead_layer"))
    current_layer = _safe_text(current_snapshot.get("lead_layer"))
    previous_title = _safe_text(previous_snapshot.get("lead_title"))
    current_title = _safe_text(current_snapshot.get("lead_title"))
    previous_detail = _safe_text(previous_snapshot.get("lead_detail"))
    current_detail = _safe_text(current_snapshot.get("lead_detail"))
    previous_impact_summary = _safe_text(previous_snapshot.get("impact_summary"))
    current_impact_summary = _safe_text(current_snapshot.get("impact_summary"))
    previous_thesis_scope = _safe_text(previous_snapshot.get("thesis_scope"))
    current_thesis_scope = _safe_text(current_snapshot.get("thesis_scope"))
    previous_importance = _safe_text(previous_snapshot.get("importance"))
    current_importance = _safe_text(current_snapshot.get("importance"))
    previous_changed_what = _safe_text(previous_snapshot.get("changed_what"))
    current_changed_what = _safe_text(current_snapshot.get("changed_what"))
    previous_importance_reason = _safe_text(previous_snapshot.get("importance_reason"))
    current_importance_reason = _safe_text(current_snapshot.get("importance_reason"))
    previous_understanding = _event_understanding_summary(previous_snapshot)
    current_understanding = _event_understanding_summary(current_snapshot)
    summary = ""
    change_type = "unchanged"

    if not previous_snapshot:
        change_type = "new_snapshot"
        summary = (
            f"thesis 里还没有上次事件快照，这次先记住当前是 `{current_status or '待补充'} / {current_layer or '新闻'}`，"
            "后续再比较到底变了什么。"
        )
        summary = _append_understanding(summary, current_understanding)
    elif previous_status != current_status:
        previous_rank = _EVENT_DIGEST_STATUS_ORDER.get(previous_status, -1)
        current_rank = _EVENT_DIGEST_STATUS_ORDER.get(current_status, -1)
        if current_rank > previous_rank:
            change_type = "status_up"
            summary = (
                f"事件状态从 `{previous_status}` 升到 `{current_status}`，"
                "说明这条线已经从补证据/待复核阶段推进到更可解释的研究层。"
            )
        else:
            change_type = "status_down"
            summary = (
                f"事件状态从 `{previous_status}` 退到 `{current_status}`，"
                "说明旧结论要先回到证据边界复核，不能直接沿用上次判断。"
            )
        summary = _append_understanding(summary, current_understanding)
    elif previous_layer != current_layer:
        change_type = "layer_rotated"
        summary = (
            f"研究前置层从 `{previous_layer or '新闻'}` 切到 `{current_layer or '新闻'}`，"
            "说明关注点已经从上一层转向新的事件传导层。"
        )
        if previous_understanding or current_understanding:
            summary += " "
            if previous_understanding:
                summary += f"上次{previous_understanding}；"
            if current_understanding:
                summary += f"这次{current_understanding}。"
    elif previous_detail != current_detail and current_detail:
        change_type = "event_detail_retyped"
        summary = (
            f"当前前置事件仍在 `{current_layer or '新闻'}` 层，但已从 `{previous_detail or '旧类型'}` 切到 `{current_detail}`，"
            "说明研究要看的传导环节已经发生了变化。"
        )
        detail_suffix = _event_detail_transition_suffix(previous_detail, current_detail)
        if detail_suffix:
            summary += detail_suffix
        if current_understanding and current_understanding not in summary:
            summary = _append_understanding(summary, current_understanding)
    elif previous_title and current_title and previous_title != current_title:
        change_type = "lead_event_replaced"
        summary = (
            f"最该前置的事件已从 `{previous_title}` 切到 `{current_title}`，"
            "说明当前研究重心已经更新，不应再只盯旧标题。"
        )
        summary = _append_understanding(summary, current_understanding)
    elif previous_thesis_scope != current_thesis_scope and current_thesis_scope:
        change_type = "thesis_scope_changed"
        summary = (
            f"这条事件对 thesis 的影响边界已从 `{previous_thesis_scope or '未定义'}` 调整到 `{current_thesis_scope}`，"
            "说明它不再只是原来的事件性质。"
        )
        detail_suffix = _event_detail_transition_suffix(previous_detail, current_detail)
        if detail_suffix:
            summary += detail_suffix
        if current_understanding and current_understanding not in summary:
            summary = _append_understanding(summary, current_understanding)
    elif previous_impact_summary != current_impact_summary and current_impact_summary:
        change_type = "impact_repriced"
        summary = (
            f"事件影响层已从 `{previous_impact_summary or '未定义'}` 调整到 `{current_impact_summary}`，"
            "这次要重新回答它到底改的是盈利、估值、景气还是资金偏好。"
        )
        detail_suffix = _event_detail_transition_suffix(previous_detail, current_detail)
        if detail_suffix:
            summary += detail_suffix
        if current_understanding and current_understanding not in summary:
            summary = _append_understanding(summary, current_understanding)
    elif previous_importance != current_importance and current_importance:
        change_type = "importance_changed"
        summary = (
            f"事件优先级已从 `{_event_importance_label(previous_importance)}` 调整到 `{_event_importance_label(current_importance)}`，"
            "说明研究前置顺序已经变化，不能再沿用上次的关注权重。"
        )
        summary = _append_understanding(summary, current_understanding)
    elif current_changed_what and previous_changed_what != current_changed_what:
        change_type = "interpretation_updated"
        summary = "虽然事件分层没换，但“这件事改变了什么”的解释已经更新，原 thesis 需要按新解释复核。"
        summary = _append_understanding(summary, current_understanding)
    elif current_importance_reason and previous_importance_reason != current_importance_reason:
        change_type = "priority_reason_updated"
        summary = "虽然事件分层没换，但事件优先级判断已经更新，现在要重新回答为什么它该前置、还是先按观察处理。"
        summary = _append_understanding(summary, current_understanding)
    else:
        summary = (
            f"当前事件重心基本延续上次判断，仍以 `{current_layer or '新闻'}` 层为主；"
            "这次更多是在确认，而不是改写 thesis。"
        )
        summary = _append_understanding(summary, current_understanding)

    return {
        "changed": _event_digest_signature(previous_snapshot) != _event_digest_signature(current_snapshot),
        "change_type": change_type,
        "summary": summary,
        "previous_understanding": previous_understanding,
        "current_understanding": current_understanding,
        "previous_snapshot": previous_snapshot,
        "current_snapshot": current_snapshot,
    }


def summarize_thesis_state_snapshot(record: Mapping[str, Any] | None) -> Dict[str, str]:
    thesis = dict(record or {})
    snapshot = dict(thesis.get("thesis_state_snapshot") or {})
    return {
        "state": _safe_text(snapshot.get("state")),
        "trigger": _safe_text(snapshot.get("trigger")),
        "summary": _safe_text(snapshot.get("summary")),
        "change_type": _safe_text(snapshot.get("change_type")),
        "updated_at": _safe_text(snapshot.get("recorded_at") or thesis.get("thesis_state_updated_at")),
    }


def build_thesis_state_transition(
    previous_record: Mapping[str, Any] | None,
    current_event_digest: Mapping[str, Any] | None,
    delta: Mapping[str, Any] | None,
    *,
    source: str = "",
    recorded_at: str = "",
) -> Dict[str, Any]:
    thesis = dict(previous_record or {})
    previous_state_snapshot = summarize_thesis_state_snapshot(thesis)
    previous_state = _safe_text(previous_state_snapshot.get("state"))
    previous_snapshot = _compact_event_digest_snapshot(thesis.get("event_digest_snapshot") or {})
    current_snapshot = _compact_event_digest_snapshot(current_event_digest)
    change_type = _safe_text(dict(delta or {}).get("change_type"))
    previous_status = _safe_text(previous_snapshot.get("status"))
    current_status = _safe_text(current_snapshot.get("status"))
    previous_layer = _safe_text(previous_snapshot.get("lead_layer"))
    current_layer = _safe_text(current_snapshot.get("lead_layer"))
    previous_detail = _safe_text(previous_snapshot.get("lead_detail"))
    current_detail = _safe_text(current_snapshot.get("lead_detail"))
    previous_thesis_scope = _safe_text(previous_snapshot.get("thesis_scope"))
    current_thesis_scope = _safe_text(current_snapshot.get("thesis_scope"))
    previous_importance = _safe_text(previous_snapshot.get("importance"))
    current_importance = _safe_text(current_snapshot.get("importance"))
    previous_layer_rank = _event_layer_rank(previous_layer)
    current_layer_rank = _event_layer_rank(current_layer)
    previous_importance_rank = _event_importance_rank(previous_importance)
    current_importance_rank = _event_importance_rank(current_importance)

    state = previous_state or "维持"
    trigger = "事件延续确认"
    summary = "当前 thesis 仍以延续确认处理，先沿用上次研究结论。"

    if not current_snapshot:
        state = "待复核"
        trigger = "缺少当前事件快照"
        summary = "当前缺少新的事件快照，旧 thesis 先退回复核，避免把空白写成确认。"
    elif current_status == "待复核":
        state = "待复核"
        trigger = "事件边界待复核"
        summary = _safe_text(dict(delta or {}).get("summary")) or "当前事件边界已退回待复核，旧 thesis 先不要按已验证继续沿用。"
    elif current_status == "待补充":
        if previous_status == "已消化" and previous_layer_rank >= _EVENT_LAYER_ORDER["政策"] and current_layer_rank <= _EVENT_LAYER_ORDER["新闻"]:
            state = "撤销"
            trigger = "高质量事件支点消失"
            summary = "原 thesis 依赖的高质量事件支点已退化成泛新闻，旧判断应先撤销，再重新搭研究框架。"
        elif previous_status in {"已消化", "待复核"} or previous_state in {"升级", "维持", "待复核"}:
            state = "削弱"
            trigger = "事件退回待补充"
            summary = "这次事件层已退回待补充，说明原 thesis 仍有方向价值，但确定性必须先下调。"
        else:
            state = "维持"
            trigger = "继续补证"
            summary = "当前 thesis 仍处在补证阶段，先维持观察，不把边界不清的事件写成新升级。"
    elif current_status == "已消化":
        if change_type in {"new_snapshot", "status_up", "lead_event_replaced"}:
            if _event_detail_is_negative(current_detail):
                state = "削弱"
                trigger = "负面事件完成消化"
                summary = _safe_text(dict(delta or {}).get("summary")) or "当前事件虽然已完成消化，但它更像直接改写 thesis 的负面压力，原结论需要先降级重审。"
            else:
                state = "升级"
                trigger = "事件完成消化"
                summary = _safe_text(dict(delta or {}).get("summary")) or "当前事件已完成消化并更新主导事件，thesis 可以按更高确定性理解。"
        elif change_type == "event_detail_retyped":
            detail_transition = _detail_retyped_transition(previous_detail, current_detail)
            if detail_transition:
                state, trigger, summary = detail_transition
            elif _event_detail_is_negative(current_detail):
                state = "削弱"
                trigger = "研究焦点转向负面压力"
                summary = "虽然事件仍是已消化，但研究焦点已经转向更直接的负面压力层，原 thesis 需要先降级重审。"
            elif current_thesis_scope == "thesis变化" and previous_thesis_scope != "thesis变化":
                state = "升级"
                trigger = "研究焦点下沉到 thesis 变化"
                summary = _safe_text(dict(delta or {}).get("summary")) or "研究焦点已经下沉到更直接的 thesis 变化层，原结论可以按更高确定性理解。"
            else:
                state = "维持"
                trigger = "研究焦点切换"
                summary = _safe_text(dict(delta or {}).get("summary")) or "当前只是研究焦点切换，thesis 暂按维持处理。"
        elif change_type == "thesis_scope_changed":
            if current_thesis_scope == "thesis变化":
                if _event_detail_is_negative(current_detail):
                    state = "削弱"
                    trigger = "负面事件升级为 thesis 变化"
                    summary = "这条事件现在已从待确认升级成直接改写 thesis 的负面压力，原结论需要先降级重审。"
                else:
                    state = "升级"
                    trigger = "事件升级为 thesis 变化"
                    summary = "这条事件现在已从噪音/待确认升级成 thesis 变化，研究结论可以按更高权重处理。"
            elif previous_thesis_scope == "thesis变化" and current_thesis_scope in {"一次性噪音", "待确认"}:
                state = "削弱"
                trigger = "事件退回非 thesis 变化"
                summary = "这条事件不再像原来那样直接改写 thesis，原结论需要先降级重审。"
            else:
                state = "维持"
                trigger = "事件边界重定价"
                summary = _safe_text(dict(delta or {}).get("summary")) or "当前只是事件边界重定价，thesis 暂按维持处理。"
        elif change_type == "impact_repriced":
            state = "削弱"
            trigger = "事件影响层改写"
            summary = "虽然事件仍是已消化，但它影响的是盈利、估值、景气还是资金偏好已经变化，原 thesis 需要先降级重审。"
        elif change_type == "importance_changed":
            if current_importance_rank > previous_importance_rank:
                state = "升级"
                trigger = "事件优先级上调"
                summary = _safe_text(dict(delta or {}).get("summary")) or "虽然事件分层没换，但研究前置顺序已上调，thesis 需要更优先复核。"
            elif current_importance_rank < previous_importance_rank:
                state = "削弱"
                trigger = "事件优先级下调"
                summary = _safe_text(dict(delta or {}).get("summary")) or "虽然事件仍在，但研究前置顺序已下调，原 thesis 要先降权观察。"
            else:
                state = "维持"
                trigger = "事件优先级重估"
                summary = _safe_text(dict(delta or {}).get("summary")) or "当前只是事件优先级重估，thesis 暂按维持处理。"
        elif change_type == "layer_rotated":
            if current_layer_rank > previous_layer_rank:
                state = "升级"
                trigger = "研究前置层下沉"
                summary = "研究前置层已从更泛的事件下沉到更直接的传导层，thesis 确定性同步上调。"
            elif current_layer_rank < previous_layer_rank:
                state = "削弱"
                trigger = "研究前置层上移"
                summary = "研究前置层从更直接的传导层退回到更泛层级，旧 thesis 要先降级再确认。"
            else:
                state = "维持"
                trigger = "研究层轮动"
                summary = _safe_text(dict(delta or {}).get("summary")) or "当前只是研究重心轮动，thesis 暂按维持处理。"
        elif change_type == "interpretation_updated":
            state = "削弱"
            trigger = "事件解释改写"
            summary = "虽然事件仍是已消化，但“改变了什么”的解释已改写，原 thesis 需要先降级重审。"
        else:
            state = "维持"
            trigger = "事件延续确认"
            summary = "当前 thesis 仍处在已消化后的延续确认阶段，先按维持处理。"

    stamp = _safe_text(recorded_at)
    snapshot = {
        "contract_version": "thesis_state.v1",
        "state": state,
        "trigger": trigger,
        "summary": summary,
        "change_type": change_type or "unchanged",
        "event_status": current_status or previous_status,
        "event_layer": current_layer or previous_layer,
        "recorded_at": stamp,
    }
    if _safe_text(source):
        snapshot["source"] = _safe_text(source)
    changed = (
        state != previous_state
        or trigger != _safe_text(previous_state_snapshot.get("trigger"))
        or summary != _safe_text(previous_state_snapshot.get("summary"))
    )
    return {**snapshot, "changed": changed}


def summarize_thesis_event_memory(record: Mapping[str, Any] | None) -> Dict[str, Any]:
    thesis = dict(record or {})
    snapshot = dict(thesis.get("event_digest_snapshot") or {})
    compact_snapshot = _compact_event_digest_snapshot(snapshot)
    status = _safe_text(compact_snapshot.get("status"))
    layer = _safe_text(compact_snapshot.get("lead_layer"))
    lead_detail = _safe_text(compact_snapshot.get("lead_detail"))
    title = _safe_text(compact_snapshot.get("lead_title"))
    importance = _safe_text(compact_snapshot.get("importance"))
    impact_summary = _safe_text(compact_snapshot.get("impact_summary"))
    thesis_scope = _safe_text(compact_snapshot.get("thesis_scope"))
    importance_reason = _safe_text(compact_snapshot.get("importance_reason"))
    changed_what = _safe_text(compact_snapshot.get("changed_what"))
    updated_at = _safe_text(snapshot.get("recorded_at") or thesis.get("event_digest_updated_at"))
    if status == "待复核":
        monitor_label = "事件待复核"
    elif status == "已消化" and layer:
        monitor_label = f"{layer}已消化"
    elif status == "待补充" and layer:
        monitor_label = f"{layer}待补证"
    elif status:
        monitor_label = f"事件{status}"
    else:
        monitor_label = "事件未跟踪"
    return {
        "status": status,
        "lead_layer": layer,
        "lead_detail": lead_detail,
        "lead_title": title,
        "importance": importance,
        "importance_label": _event_importance_label(importance),
        "impact_summary": impact_summary,
        "thesis_scope": thesis_scope,
        "importance_reason": importance_reason,
        "changed_what": changed_what,
        "updated_at": updated_at,
        "ledger_size": len(thesis.get("event_digest_ledger") or []),
        "monitor_label": monitor_label,
    }


def summarize_thesis_review_priority(
    record: Mapping[str, Any] | None,
    *,
    weight: float = 0.0,
    pnl: float = 0.0,
) -> Dict[str, Any]:
    thesis = dict(record or {})
    if not thesis:
        return {
            "priority": "高",
            "score": 100,
            "summary": "还没有绑定 thesis，无法持续复查原始判断。",
        }

    event_memory = summarize_thesis_event_memory(thesis)
    state_memory = summarize_thesis_state_snapshot(thesis)
    event_status = _safe_text(event_memory.get("status"))
    thesis_state = _safe_text(state_memory.get("state"))
    score = {
        "待复核": 94,
        "撤销": 90,
        "削弱": 76,
        "维持": 38,
        "升级": 32,
    }.get(
        thesis_state,
        {
            "待复核": 92,
            "待补充": 68,
            "已消化": 36,
        }.get(event_status, 52),
    )
    reasons: List[str] = []

    if thesis_state == "待复核":
        reasons.append("thesis 待复核")
    elif thesis_state == "撤销":
        reasons.append("thesis 已撤销")
    elif thesis_state == "削弱":
        reasons.append("thesis 已削弱")
    elif thesis_state == "维持":
        reasons.append("thesis 维持")
    elif thesis_state == "升级":
        reasons.append("thesis 已升级")

    if event_status == "待复核":
        reasons.append("事件边界待复核")
    elif event_status == "待补充":
        reasons.append("事件层待补证")
    elif event_status == "已消化":
        reasons.append("事件层已消化")
    else:
        reasons.append("事件层尚未形成稳定快照")

    if weight >= 0.25:
        score += 8
        reasons.append("仓位较重")
    elif weight >= 0.12:
        score += 4
        reasons.append("已有一定仓位")

    if pnl <= -0.08:
        score += 10
        reasons.append("浮亏已扩大")
    elif pnl <= -0.03:
        score += 4
        reasons.append("价格开始偏离")

    priority = "低"
    for threshold, label in _REVIEW_PRIORITY_LABELS:
        if score >= threshold:
            priority = label
            break

    return {
        "priority": priority,
        "score": score,
        "summary": "，".join(reasons),
        "event_monitor_label": _safe_text(event_memory.get("monitor_label")),
        "event_importance": _safe_text(event_memory.get("importance")),
        "event_importance_label": _safe_text(event_memory.get("importance_label")),
        "event_importance_reason": _safe_text(event_memory.get("importance_reason")),
        "impact_summary": _safe_text(event_memory.get("impact_summary")),
        "thesis_scope": _safe_text(event_memory.get("thesis_scope")),
        "thesis_state": thesis_state,
        "thesis_state_trigger": _safe_text(state_memory.get("trigger")),
        "thesis_state_summary": _safe_text(state_memory.get("summary")),
    }


def build_thesis_review_queue(items: List[Mapping[str, Any]] | None) -> List[Dict[str, Any]]:
    queue: List[Dict[str, Any]] = []
    for raw in list(items or []):
        row = dict(raw or {})
        symbol = _safe_text(row.get("symbol"))
        if not symbol:
            continue
        thesis = row.get("record")
        event_memory = summarize_thesis_event_memory(thesis)
        priority = summarize_thesis_review_priority(
            thesis,
            weight=float(row.get("weight", 0.0) or 0.0),
            pnl=float(row.get("pnl", 0.0) or 0.0),
        )
        queue.append(
            {
                "symbol": symbol,
                "has_thesis": bool(thesis),
                "priority": _safe_text(priority.get("priority")) or "低",
                "score": int(priority.get("score", 0) or 0),
                "summary": _safe_text(priority.get("summary")),
                "thesis_state": _safe_text(priority.get("thesis_state")),
                "thesis_state_trigger": _safe_text(priority.get("thesis_state_trigger")),
                "thesis_state_summary": _safe_text(priority.get("thesis_state_summary")),
                "event_layer": _safe_text(event_memory.get("lead_layer")),
                "event_detail": _safe_text(event_memory.get("lead_detail")),
                "event_importance": _safe_text(priority.get("event_importance")),
                "event_importance_label": _safe_text(priority.get("event_importance_label")),
                "event_importance_reason": _safe_text(priority.get("event_importance_reason")),
                "impact_summary": _safe_text(priority.get("impact_summary")),
                "thesis_scope": _safe_text(priority.get("thesis_scope")),
                "event_monitor_label": _safe_text(priority.get("event_monitor_label")),
                "recommended_action": _review_queue_recommended_action(
                    {
                        "symbol": symbol,
                        "has_thesis": bool(thesis),
                        "summary": _safe_text(priority.get("summary")),
                        "thesis_state": _safe_text(priority.get("thesis_state")),
                        "thesis_state_trigger": _safe_text(priority.get("thesis_state_trigger")),
                        "thesis_scope": _safe_text(priority.get("thesis_scope")),
                        "event_monitor_label": _safe_text(priority.get("event_monitor_label")),
                    }
                ),
            }
        )
    queue.sort(
        key=lambda item: (
            -int(item.get("score", 0) or 0),
            {"高": 0, "中": 1, "低": 2}.get(_safe_text(item.get("priority")), 3),
            _safe_text(item.get("symbol")),
        )
    )
    return queue


def _review_queue_date_key(value: Any) -> str:
    text = _safe_text(value)
    if len(text) >= 10:
        return text[:10]
    return datetime.now().strftime("%Y-%m-%d")


def _review_queue_recommended_action(item: Mapping[str, Any]) -> str:
    row = dict(item or {})
    if not bool(row.get("has_thesis")):
        return "补 thesis"
    thesis_state = _safe_text(row.get("thesis_state"))
    if thesis_state in {"待复核", "削弱"}:
        return "重跑 scan"
    if thesis_state == "撤销":
        return "复查 thesis"
    blob = " ".join(
        part for part in (_safe_text(row.get("summary")), _safe_text(row.get("event_monitor_label"))) if part
    )
    if "待复核" in blob:
        return "重跑 scan"
    return "复查 thesis"


def _review_queue_followup_command(item: Mapping[str, Any]) -> str:
    row = dict(item or {})
    symbol = _safe_text(row.get("symbol"))
    if not symbol:
        return ""
    followup = dict(row.get("report_followup") or {})
    followup_status = _safe_text(followup.get("status"))
    reports = [dict(entry or {}) for entry in list(followup.get("reports") or []) if dict(entry or {})]
    report_type = _safe_text(reports[0].get("report_type")) if reports else ""
    if not report_type:
        artifact_path = _safe_text(dict(row.get("last_run") or {}).get("artifact_path"))
        lowered = artifact_path.lower()
        if "stock_analysis" in lowered:
            report_type = "stock_analysis"
        elif "scan" in lowered:
            report_type = "scan"
    if followup_status in {"待更新正式稿", "已有复查稿，暂无正式稿"}:
        if report_type == "scan":
            return f"python -m src.commands.scan {symbol} --client-final"
        if report_type == "stock_analysis":
            return f"python -m src.commands.stock_analysis {symbol} --client-final"
    action = _safe_text(row.get("recommended_action")) or _review_queue_recommended_action(row)
    if action == "重跑 scan":
        return f"python -m src.commands.scan {symbol}"
    if action == "补 thesis":
        return f"python -m src.commands.research {symbol} 现在为什么值得继续跟踪"
    return f"python -m src.commands.research {symbol} 这次什么变了"


def build_review_queue_action_items(
    queue: List[Mapping[str, Any]] | None,
    *,
    limit: int = 2,
) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    selected = [dict(item or {}) for item in list(queue or []) if _safe_text(dict(item or {}).get("priority")) != "低"]
    for row in selected[: max(limit, 0)]:
        symbol = _safe_text(row.get("symbol"))
        if not symbol:
            continue
        priority = _safe_text(row.get("priority")) or "中"
        followup = dict(row.get("report_followup") or {})
        followup_status = _safe_text(followup.get("status"))
        followup_reason = _safe_text(followup.get("reason"))
        if followup_status in {"待更新正式稿", "已有复查稿，暂无正式稿"}:
            action = "补正式稿"
        else:
            action = _safe_text(row.get("recommended_action")) or _review_queue_recommended_action(row)
        command = _review_queue_followup_command(row)
        detail = "；".join(
            part
            for part in (
                (f"最近正式稿状态 {followup_status}" if followup_status else ""),
                followup_reason,
                _safe_text(row.get("event_detail")),
                (
                    _safe_text(row.get("thesis_state_trigger"))
                    if _safe_text(row.get("thesis_state_trigger"))
                    else ""
                ),
                _safe_text(row.get("thesis_state_summary")),
                (
                    f"事件优先级 {_safe_text(row.get('event_importance_label'))}"
                    if _safe_text(row.get("event_importance_label"))
                    else ""
                ),
                _safe_text(row.get("impact_summary")),
                _safe_text(row.get("thesis_scope")),
                _safe_text(row.get("event_importance_reason")),
                _safe_text(row.get("summary")),
                _safe_text(row.get("event_monitor_label")),
            )
            if part
        )
        items.append(
            {
                "symbol": symbol,
                "priority": priority,
                "recommended_action": action,
                "command": command,
                "detail": detail,
            }
        )
    return items


def summarize_review_queue_followup_lines(
    queue: List[Mapping[str, Any]] | None,
    *,
    limit: int = 2,
) -> List[str]:
    lines: List[str] = []
    selected = [dict(item or {}) for item in list(queue or []) if dict(item or {}).get("report_followup")]
    for row in selected[: max(limit, 0)]:
        symbol = _safe_text(row.get("symbol"))
        if not symbol:
            continue
        followup = dict(row.get("report_followup") or {})
        followup_status = _safe_text(followup.get("status"))
        if followup_status not in {"待更新正式稿", "已有复查稿，暂无正式稿"}:
            continue
        reason = _safe_text(followup.get("reason"))
        command = _review_queue_followup_command(row)
        line = f"正式稿跟进: {symbol} 当前是 `{followup_status}`"
        if reason:
            line += f"，{reason}"
        if command:
            line += f"；建议命令：`{command}`"
        lines.append(line + "。")
    return lines


def enrich_review_queue_with_history(
    queue: List[Mapping[str, Any]] | None,
    history_payload: Mapping[str, Any] | None,
) -> List[Dict[str, Any]]:
    payload = dict(history_payload or {})
    active_map = {
        _safe_text(dict(item or {}).get("symbol")): dict(item or {})
        for item in list(payload.get("active") or [])
        if _safe_text(dict(item or {}).get("symbol"))
    }
    history_map = {
        _safe_text(key): dict(value or {})
        for key, value in dict(payload.get("history") or payload).items()
        if _safe_text(key)
    }
    if not active_map and not history_map:
        return [dict(item or {}) for item in list(queue or [])]
    enriched: List[Dict[str, Any]] = []
    for item in list(queue or []):
        row = dict(item or {})
        symbol = _safe_text(row.get("symbol"))
        active = dict(active_map.get(symbol) or {})
        history = dict(history_map.get(symbol) or {})
        if active.get("report_followup"):
            row["report_followup"] = active.get("report_followup")
        elif history.get("report_followup"):
            row["report_followup"] = history.get("report_followup")
        if active.get("last_run"):
            row["last_run"] = active.get("last_run")
        elif history.get("last_run"):
            row["last_run"] = history.get("last_run")
        enriched.append(row)
    return enriched


def summarize_review_queue_summary_line(
    queue: List[Mapping[str, Any]] | None,
    *,
    prefix: str = "优先复查 thesis:",
    limit: int = 2,
) -> str:
    selected = [dict(item or {}) for item in list(queue or []) if _safe_text(dict(item or {}).get("priority")) != "低"]
    if not selected:
        return ""
    rendered = "；".join(
        f"{_safe_text(item.get('symbol'))}（{_safe_text(item.get('priority')) or '中'}）"
        + (
            " "
            + "；".join(
                part
                for part in (
                    _safe_text(item.get("thesis_state")),
                    _safe_text(item.get("thesis_state_trigger")),
                    _safe_text(item.get("thesis_state_summary")),
                    _safe_text(item.get("thesis_scope")),
                    _safe_text(item.get("event_detail")),
                    (
                        f"事件优先级 {_safe_text(item.get('event_importance_label'))}"
                        if _safe_text(item.get("event_importance_label"))
                        else ""
                    ),
                    _safe_text(item.get("impact_summary")),
                    _safe_text(item.get("summary")),
                    _safe_text(item.get("event_monitor_label")),
                )
                if part
            )
            if any(
                _safe_text(item.get(key))
                for key in (
                    "thesis_state",
                    "thesis_state_trigger",
                    "thesis_state_summary",
                    "thesis_scope",
                    "event_detail",
                    "event_importance_label",
                    "impact_summary",
                    "summary",
                    "event_monitor_label",
                )
            )
            else ""
        )
        for item in selected[: max(limit, 0)]
        if _safe_text(item.get("symbol"))
    )
    if not rendered:
        return ""
    return (
        f"{prefix} {rendered}。".strip()
    )


def summarize_review_queue_action_lines(
    queue: List[Mapping[str, Any]] | None,
    *,
    limit: int = 2,
) -> List[str]:
    lines: List[str] = []
    for index, row in enumerate(build_review_queue_action_items(queue, limit=limit), start=1):
        line = f"研究动作 {index}: {row['symbol']}（{row['priority']}）先{row['recommended_action']}"
        detail = _safe_text(row.get("detail"))
        if detail:
            line += f"，原因：{detail}"
        command = _safe_text(row.get("command"))
        if command:
            line += f"；命令：`{command}`"
        lines.append(line + "。")
    return lines


def summarize_review_queue_transitions(payload: Mapping[str, Any] | None) -> List[str]:
    record = dict(payload or {})
    lines: List[str] = []

    def _render_focus(item: Mapping[str, Any]) -> str:
        return "；".join(
            part
            for part in (
                _safe_text(item.get("event_detail")),
                _safe_text(item.get("thesis_state_trigger")),
                _safe_text(item.get("thesis_state_summary")),
                (
                    f"事件优先级 {_safe_text(item.get('event_importance_label'))}"
                    if _safe_text(item.get("event_importance_label"))
                    else ""
                ),
                _safe_text(item.get("impact_summary")),
                _safe_text(item.get("thesis_scope")),
            )
            if part
        )

    def _render_items(items: List[Mapping[str, Any]]) -> str:
        return "；".join(
            f"{_safe_text(item.get('symbol'))}（{_safe_text(item.get('priority'))}"
            + (
                f"，建议{_safe_text(item.get('recommended_action'))}"
                if _safe_text(item.get("recommended_action"))
                else ""
            )
            + (
                f"，焦点：{_render_focus(item)}"
                if _render_focus(item)
                else ""
            )
            + "）"
            for item in items
            if _safe_text(item.get("symbol"))
        )

    new_entries = list(record.get("new_entries") or [])
    if new_entries:
        lines.append("今日新进复查队列: " + _render_items(new_entries[:3]) + "。")

    resolved_entries = list(record.get("resolved_entries") or [])
    if resolved_entries:
        lines.append("今日移出复查队列: " + _render_items(resolved_entries[:3]) + "。")

    stale_entries = list(record.get("stale_high_priority") or [])
    if stale_entries:
        rendered = "；".join(
            f"{_safe_text(item.get('symbol'))}（已连续 {_safe_text(item.get('active_days'))} 天高优先级，建议{_safe_text(item.get('recommended_action'))}）"
            + (
                f" 焦点：{_render_focus(item)}"
                if _render_focus(item)
                else ""
            )
            for item in stale_entries[:3]
            if _safe_text(item.get("symbol"))
        )
        if rendered:
            lines.append("连续高优先级未处理: " + rendered + "。")
    return lines


def summarize_review_queue_history_lines(payload: Mapping[str, Any] | None) -> List[str]:
    record = dict(payload or {})
    lines: List[str] = []

    focus = "；".join(
        part
        for part in (
            _safe_text(record.get("event_detail")),
            _safe_text(record.get("thesis_state_trigger")),
            (
                f"事件优先级 {_safe_text(record.get('event_importance_label'))}"
                if _safe_text(record.get("event_importance_label"))
                else ""
            ),
            _safe_text(record.get("impact_summary")),
            _safe_text(record.get("thesis_scope")),
        )
        if part
    )
    if focus:
        lines.append(f"最近复查焦点: {focus}")
    state_trigger = _safe_text(record.get("thesis_state_trigger"))
    if state_trigger:
        lines.append(f"最近状态触发: {state_trigger}")
    state_summary = _safe_text(record.get("thesis_state_summary"))
    if state_summary:
        lines.append(f"最近状态解释: {state_summary}")
    importance_reason = _safe_text(record.get("event_importance_reason"))
    if importance_reason:
        lines.append(f"最近优先级判断: {importance_reason}")

    followup = dict(record.get("report_followup") or {})
    followup_status = _safe_text(followup.get("status"))
    followup_reason = _safe_text(followup.get("reason"))
    if followup_status:
        lines.append(f"最近正式稿状态: {followup_status}")
    if followup_reason:
        lines.append(f"正式稿跟进说明: {followup_reason}")
    for index, item in enumerate(list(followup.get("reports") or [])[:2], start=1):
        row = dict(item or {})
        markdown = _safe_text(row.get("markdown"))
        if not markdown:
            continue
        report_type = _safe_text(row.get("report_type")) or "report"
        generated_at = _safe_text(row.get("generated_at")) or "—"
        lines.append(f"最近正式稿 {index}: {report_type} / {generated_at} / `{markdown}`")

    last_run = dict(record.get("last_run") or {})
    last_action = _safe_text(last_run.get("action"))
    if last_action:
        lines.append(f"最近复查动作: {last_action}")
    last_status = _safe_text(last_run.get("status"))
    if last_status:
        lines.append(f"最近复查结果: {last_status}")
    last_recorded_at = _safe_text(last_run.get("recorded_at"))
    if last_recorded_at:
        lines.append(f"最近复查时间: {last_recorded_at}")
    last_summary = _safe_text(last_run.get("summary"))
    if last_summary:
        lines.append(f"最近复查摘要: {last_summary}")
    artifact_path = _safe_text(last_run.get("artifact_path"))
    if artifact_path:
        lines.append(f"最近复查产物: `{artifact_path}`")
    return lines


def lookup_latest_symbol_reports(
    symbols: List[str] | None,
    *,
    reviews_root: Path = PROJECT_ROOT / "reports" / "reviews",
    limit: int = 2,
) -> Dict[str, List[Dict[str, str]]]:
    targets = {_safe_text(symbol) for symbol in list(symbols or []) if _safe_text(symbol)}
    if not targets or not reviews_root.exists():
        return {}

    refs: Dict[str, List[Dict[str, str]]] = {symbol: [] for symbol in targets}
    for path in reviews_root.rglob("*__release_manifest.json"):
        payload = load_json(path, default={}) or {}
        artifacts = dict(payload.get("artifacts") or {})
        symbol = _safe_text(artifacts.get("symbol") or payload.get("symbol"))
        if symbol not in targets:
            continue
        refs[symbol].append(
            {
                "report_type": _safe_text(payload.get("report_type")),
                "generated_at": _safe_text(payload.get("generated_at")),
                "markdown": _safe_text(payload.get("markdown")),
                "manifest": str(path),
            }
        )

    for symbol, rows in refs.items():
        rows.sort(
            key=lambda item: (
                _safe_text(item.get("generated_at")),
                _safe_text(item.get("markdown")),
            ),
            reverse=True,
        )
        refs[symbol] = rows[: max(limit, 0)]
    return {symbol: rows for symbol, rows in refs.items() if rows}


def _report_followup_payload(
    row: Mapping[str, Any] | None,
    report_refs: List[Mapping[str, Any]] | None,
    *,
    state: str,
    updated_at: str,
) -> Dict[str, Any]:
    refs = [dict(item or {}) for item in list(report_refs or []) if _safe_text(dict(item or {}).get("markdown"))]
    summary = _safe_text(dict(row or {}).get("summary"))
    event_label = _safe_text(dict(row or {}).get("event_monitor_label"))
    detail = "；".join(part for part in (summary, event_label) if part)
    if state == "active":
        status = "需复查" if refs else "暂无正式稿"
        reason = detail or "当前 thesis 已进入复查队列。"
    else:
        status = "已复核" if refs else "暂无正式稿"
        reason = "最近一轮已移出复查队列。"
    return {
        "status": status,
        "reason": reason,
        "updated_at": _safe_text(updated_at),
        "reports": refs[:2],
    }


def _refresh_report_followup_after_run(
    followup: Mapping[str, Any] | None,
    run_payload: Mapping[str, Any] | None,
) -> Dict[str, Any]:
    payload = dict(followup or {})
    refs = [dict(item or {}) for item in list(payload.get("reports") or []) if _safe_text(dict(item or {}).get("markdown"))]
    status = _safe_text(dict(run_payload or {}).get("status")).lower()
    artifact_path = _safe_text(dict(run_payload or {}).get("artifact_path"))
    summary = _safe_text(dict(run_payload or {}).get("summary"))
    recorded_at = _safe_text(dict(run_payload or {}).get("recorded_at"))

    if artifact_path:
        payload["latest_review_artifact"] = artifact_path
    if recorded_at:
        payload["last_reviewed_at"] = recorded_at

    if status not in {"completed", "success", "done", "ok"}:
        return payload

    artifact_matches_final = False
    if artifact_path:
        artifact_matches_final = any(_safe_text(ref.get("markdown")) == artifact_path for ref in refs)
        if not artifact_matches_final:
            lowered = artifact_path.lower()
            artifact_matches_final = "client_final" in lowered or lowered.endswith("_final.md")

    if refs and artifact_matches_final:
        payload["status"] = "已复核"
        payload["reason"] = summary or "最近复查已完成，且最新产物已经更新到正式稿。"
        payload["needs_refresh"] = False
        payload["stale"] = False
        payload["updated_at"] = recorded_at or _safe_text(payload.get("updated_at"))
        return payload

    if refs:
        payload["status"] = "待更新正式稿"
        payload["reason"] = summary or "最近复查已完成，但当前正式稿仍停留在旧版本；下一步应补新的 final / client_final。"
        payload["needs_refresh"] = True
        payload["stale"] = True
        payload["updated_at"] = recorded_at or _safe_text(payload.get("updated_at"))
        return payload

    payload["status"] = "已有复查稿，暂无正式稿"
    payload["reason"] = summary or "最近复查已完成并已落复查稿，但还没有新的正式稿可沿用。"
    payload["needs_refresh"] = True
    payload["stale"] = False
    payload["updated_at"] = recorded_at or _safe_text(payload.get("updated_at"))
    return payload


class ThesisRepository:
    """JSON-backed thesis storage."""

    def __init__(
        self,
        thesis_path: Path = PROJECT_ROOT / "data" / "thesis.json",
        review_queue_path: Path = PROJECT_ROOT / "data" / "thesis_review_queue.json",
    ) -> None:
        self.thesis_path = thesis_path
        self.review_queue_path = review_queue_path

    def load(self) -> Dict[str, Any]:
        return load_json(self.thesis_path, default={}) or {}

    def save(self, payload: Dict[str, Any]) -> None:
        save_json(self.thesis_path, payload)

    def load_review_queue(self) -> Dict[str, Any]:
        payload = load_json(self.review_queue_path, default=_default_review_queue_ledger())
        if not payload:
            payload = _default_review_queue_ledger()
        payload.setdefault("contract_version", "thesis_review_queue.v1")
        payload.setdefault("active", [])
        payload.setdefault("history", {})
        payload.setdefault("last_updated_at", "")
        payload.setdefault("last_source", "")
        payload.setdefault("last_transitions", {"new_entries": [], "resolved_entries": [], "stale_high_priority": []})
        return payload

    def save_review_queue(self, payload: Dict[str, Any]) -> None:
        save_json(self.review_queue_path, payload)

    def get(self, symbol: str):
        return self.load().get(symbol)

    def list_all(self) -> List[Dict[str, Any]]:
        payload = self.load()
        return [{**value, "symbol": key} for key, value in sorted(payload.items())]

    def upsert(
        self,
        symbol: str,
        core_assumption: str,
        validation_metric: str,
        stop_condition: str,
        holding_period: str,
    ) -> Dict[str, Any]:
        payload = self.load()
        existing = payload.get(symbol, {})
        record = {
            **existing,
            "core_assumption": core_assumption,
            "validation_metric": validation_metric,
            "stop_condition": stop_condition,
            "holding_period": holding_period,
            "created_at": existing.get("created_at", datetime.now().strftime("%Y-%m-%d")),
            "updated_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        }
        payload[symbol] = record
        self.save(payload)
        return {"symbol": symbol, **record}

    def record_event_digest(
        self,
        symbol: str,
        event_digest: Mapping[str, Any],
        *,
        source: str = "",
        recorded_at: str = "",
    ) -> Dict[str, Any]:
        payload = self.load()
        existing = dict(payload.get(symbol) or {})
        if not existing:
            return {}

        previous_snapshot = dict(existing.get("event_digest_snapshot") or {})
        current_snapshot = _compact_event_digest_snapshot(event_digest)
        if not current_snapshot:
            return {}

        stamp = recorded_at or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        delta = compare_event_digest_snapshots(previous_snapshot, current_snapshot)
        snapshot_with_meta = {
            **current_snapshot,
            "recorded_at": stamp,
        }
        if _safe_text(source):
            snapshot_with_meta["source"] = _safe_text(source)
        state_transition = build_thesis_state_transition(
            existing,
            current_snapshot,
            delta,
            source=source,
            recorded_at=stamp,
        )

        record = {
            **existing,
            "event_digest_snapshot": snapshot_with_meta,
            "event_digest_updated_at": stamp,
            "thesis_state_snapshot": {key: value for key, value in state_transition.items() if key != "changed"},
            "thesis_state_updated_at": stamp,
        }
        if _safe_text(source):
            record["event_digest_source"] = _safe_text(source)
            record["thesis_state_source"] = _safe_text(source)

        previous_signature = _event_digest_signature(previous_snapshot)
        current_signature = _event_digest_signature(current_snapshot)
        ledger = list(existing.get("event_digest_ledger") or [])
        if previous_signature != current_signature:
            ledger.append(
                {
                    "recorded_at": stamp,
                    "source": _safe_text(source),
                    "snapshot": current_snapshot,
                    "delta": {
                        "change_type": _safe_text(delta.get("change_type")),
                        "summary": _safe_text(delta.get("summary")),
                    },
                }
            )
        if ledger:
            record["event_digest_ledger"] = ledger[-12:]
        state_ledger = list(existing.get("thesis_state_ledger") or [])
        if bool(state_transition.get("changed")):
            state_ledger.append({key: value for key, value in state_transition.items() if key != "changed"})
        if state_ledger:
            record["thesis_state_ledger"] = state_ledger[-12:]

        payload[symbol] = record
        self.save(payload)
        return {
            "symbol": symbol,
            "snapshot": snapshot_with_meta,
            "previous_snapshot": previous_snapshot,
            "delta": delta,
            "ledger_size": len(record.get("event_digest_ledger") or []),
            "state_transition": state_transition,
            "state_ledger_size": len(record.get("thesis_state_ledger") or []),
        }

    def record_review_queue(
        self,
        queue: List[Mapping[str, Any]],
        *,
        source: str = "",
        as_of: str = "",
    ) -> Dict[str, Any]:
        ledger = self.load_review_queue()
        stamp = _safe_text(as_of) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        current_date = _review_queue_date_key(stamp)
        active_rows = [dict(item or {}) for item in list(queue or []) if _safe_text(dict(item or {}).get("priority")) != "低"]
        previous_active = {str(item.get("symbol", "")): dict(item) for item in list(ledger.get("active") or [])}
        history = dict(ledger.get("history") or {})

        new_entries: List[Dict[str, Any]] = []
        stale_high_priority: List[Dict[str, Any]] = []
        current_symbols: set[str] = set()
        normalized_active: List[Dict[str, Any]] = []

        for raw in active_rows:
            symbol = _safe_text(raw.get("symbol"))
            if not symbol:
                continue
            current_symbols.add(symbol)
            previous_history = dict(history.get(symbol) or {})
            was_resolved = _safe_text(previous_history.get("last_state")) == "resolved"
            last_seen_date = _safe_text(previous_history.get("last_seen_date"))
            active_days = 0 if was_resolved else int(previous_history.get("active_days", 0) or 0)
            if last_seen_date != current_date:
                active_days += 1
            normalized = {
                "symbol": symbol,
                "priority": _safe_text(raw.get("priority")),
                "score": int(raw.get("score", 0) or 0),
                "summary": _safe_text(raw.get("summary")),
                "thesis_state": _safe_text(raw.get("thesis_state")),
                "thesis_state_trigger": _safe_text(raw.get("thesis_state_trigger")),
                "thesis_state_summary": _safe_text(raw.get("thesis_state_summary")),
                "event_layer": _safe_text(raw.get("event_layer")),
                "event_detail": _safe_text(raw.get("event_detail")),
                "event_importance": _safe_text(raw.get("event_importance")),
                "event_importance_label": _safe_text(raw.get("event_importance_label")),
                "event_importance_reason": _safe_text(raw.get("event_importance_reason")),
                "impact_summary": _safe_text(raw.get("impact_summary")),
                "thesis_scope": _safe_text(raw.get("thesis_scope")),
                "event_monitor_label": _safe_text(raw.get("event_monitor_label")),
                "has_thesis": bool(raw.get("has_thesis")),
                "recommended_action": _review_queue_recommended_action(raw),
                "first_seen_at": stamp if was_resolved else (_safe_text(previous_history.get("first_seen_at")) or stamp),
                "last_seen_at": stamp,
                "active_days": active_days,
            }
            normalized_active.append(normalized)
            if symbol not in previous_active:
                new_entries.append(dict(normalized))
            if normalized["priority"] == "高" and active_days >= 3:
                stale_high_priority.append(dict(normalized))
            history[symbol] = {
                **previous_history,
                "symbol": symbol,
                "first_seen_at": normalized["first_seen_at"],
                "last_seen_at": stamp,
                "last_seen_date": current_date,
                "active_days": active_days,
                "priority": normalized["priority"],
                "thesis_state": normalized["thesis_state"],
                "thesis_state_trigger": normalized["thesis_state_trigger"],
                "thesis_state_summary": normalized["thesis_state_summary"],
                "event_detail": normalized["event_detail"],
                "event_importance": normalized["event_importance"],
                "event_importance_label": normalized["event_importance_label"],
                "event_importance_reason": normalized["event_importance_reason"],
                "impact_summary": normalized["impact_summary"],
                "thesis_scope": normalized["thesis_scope"],
                "recommended_action": normalized["recommended_action"],
                "last_state": "active",
            }

        resolved_entries: List[Dict[str, Any]] = []
        for symbol, raw in previous_active.items():
            if symbol in current_symbols:
                continue
            resolved = {
                "symbol": symbol,
                "priority": _safe_text(raw.get("priority")),
                "summary": _safe_text(raw.get("summary")),
                "thesis_state": _safe_text(raw.get("thesis_state")),
                "thesis_state_trigger": _safe_text(raw.get("thesis_state_trigger")),
                "thesis_state_summary": _safe_text(raw.get("thesis_state_summary")),
                "event_layer": _safe_text(raw.get("event_layer")),
                "event_detail": _safe_text(raw.get("event_detail")),
                "event_importance": _safe_text(raw.get("event_importance")),
                "event_importance_label": _safe_text(raw.get("event_importance_label")),
                "event_importance_reason": _safe_text(raw.get("event_importance_reason")),
                "impact_summary": _safe_text(raw.get("impact_summary")),
                "thesis_scope": _safe_text(raw.get("thesis_scope")),
                "event_monitor_label": _safe_text(raw.get("event_monitor_label")),
                "recommended_action": _safe_text(raw.get("recommended_action")) or _review_queue_recommended_action(raw),
            }
            resolved_entries.append(resolved)
            previous_history = dict(history.get(symbol) or {})
            history[symbol] = {
                **previous_history,
                "last_resolved_at": stamp,
                "last_state": "resolved",
            }

        report_refs = lookup_latest_symbol_reports(list(current_symbols | set(previous_active.keys())))
        for item in normalized_active:
            symbol = _safe_text(item.get("symbol"))
            followup = _report_followup_payload(item, report_refs.get(symbol) or [], state="active", updated_at=stamp)
            item["report_followup"] = followup
            history[symbol] = {
                **dict(history.get(symbol) or {}),
                "report_followup": followup,
            }
        for item in resolved_entries:
            symbol = _safe_text(item.get("symbol"))
            followup = _report_followup_payload(item, report_refs.get(symbol) or [], state="resolved", updated_at=stamp)
            history[symbol] = {
                **dict(history.get(symbol) or {}),
                "report_followup": followup,
            }

        ledger.update(
            {
                "contract_version": "thesis_review_queue.v1",
                "active": normalized_active,
                "history": history,
                "last_updated_at": stamp,
                "last_source": _safe_text(source),
                "last_transitions": {
                    "new_entries": new_entries,
                    "resolved_entries": resolved_entries,
                    "stale_high_priority": stale_high_priority,
                },
            }
        )
        self.save_review_queue(ledger)
        return {
            "contract_version": "thesis_review_queue.v1",
            "as_of": stamp,
            "source": _safe_text(source),
            "active": normalized_active,
            "new_entries": new_entries,
            "resolved_entries": resolved_entries,
            "stale_high_priority": stale_high_priority,
        }

    def record_review_run(
        self,
        symbol: str,
        *,
        action: str = "",
        status: str = "",
        artifact_path: str = "",
        summary: str = "",
        recorded_at: str = "",
    ) -> Dict[str, Any]:
        key = _safe_text(symbol)
        if not key:
            return {}
        ledger = self.load_review_queue()
        history = dict(ledger.get("history") or {})
        previous = dict(history.get(key) or {})
        stamp = _safe_text(recorded_at) or datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        run_payload = {
            "action": _safe_text(action),
            "status": _safe_text(status) or "completed",
            "artifact_path": _safe_text(artifact_path),
            "summary": _safe_text(summary),
            "recorded_at": stamp,
        }
        history[key] = {
            **previous,
            "symbol": key,
            "report_followup": _refresh_report_followup_after_run(previous.get("report_followup"), run_payload),
            "last_run": run_payload,
        }
        ledger["history"] = history
        ledger["last_updated_at"] = stamp
        self.save_review_queue(ledger)
        return run_payload

    def delete(self, symbol: str) -> bool:
        payload = self.load()
        if symbol not in payload:
            return False
        del payload[symbol]
        self.save(payload)
        return True
