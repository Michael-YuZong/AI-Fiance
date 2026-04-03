"""Markdown renderers for strategy prediction ledger."""

from __future__ import annotations

from typing import Any, Iterable, List, Mapping, Sequence


def _pct(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return "—"
    return f"{number * 100:+.2f}%"


def _ratio(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return "—"
    return f"{number:.1%}"


def _table(headers: Sequence[str], rows: Iterable[Sequence[Any]]) -> List[str]:
    lines = [
        "| " + " | ".join(str(header) for header in headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        cells = [str(cell).replace("|", "\\|").replace("\n", "<br>") for cell in row]
        lines.append("| " + " | ".join(cells) + " |")
    return lines


def _section_lead_lines(text: str) -> List[str]:
    line = str(text).strip()
    if not line:
        return []
    return [f"**先看结论：** {line}", ""]


_BENCHMARK_FIXTURE_BLOCKERS = {
    "benchmark_missing": "基准历史缺失",
    "benchmark_overlap_insufficient": "overlap 不足",
    "benchmark_as_of_misaligned": "as_of 未对齐",
    "asset_history_missing": "标的历史缺失",
}

_LAG_VISIBILITY_STATUS_LABELS = {
    "ready": "已就绪",
    "partial": "部分就绪",
    "blocked": "阻断",
    "missing": "缺失",
    "not_applicable": "不适用",
}

_OVERLAP_STATUS_LABELS = {
    "ready": "通过",
    "blocked": "重叠",
    "missing": "缺失",
}

_PROMOTION_GATE_STATUS_LABELS = {
    "blocked": "阻断",
    "stay_on_baseline": "保留 baseline",
    "queue_for_next_stage": "进入下一阶段",
}

_PROMOTION_GATE_REASON_LABELS = {
    "variant_rows_missing": "缺少 variant rows",
    "baseline_missing": "缺少 baseline",
    "overlap_fixture_blocked": "overlap fixture 未通过",
    "sample_count_below_floor": "样本数低于 gate 下限",
    "baseline_validated_rows_below_floor": "baseline validated rows 不足",
    "candidate_validated_rows_below_floor": "challenger validated rows 不足",
    "baseline_out_of_sample_blocked": "baseline out-of-sample 未就绪",
    "candidate_out_of_sample_blocked": "challenger out-of-sample 未就绪",
    "baseline_cross_sectional_blocked": "baseline cross-sectional 未就绪",
    "candidate_cross_sectional_blocked": "challenger cross-sectional 未就绪",
    "challenger_missing": "没有可比较 challenger",
    "baseline_still_best": "baseline 仍是当前最优",
    "candidate_out_of_sample_not_stable": "challenger out-of-sample 还不稳定",
    "candidate_cross_sectional_not_stable": "challenger cross-sectional 还不稳定",
    "primary_score_edge_too_small": "primary score 优势不足",
    "hit_rate_not_improved": "hit rate 没有改善",
    "avg_excess_return_not_improved": "平均超额收益没有改善",
    "avg_cost_adjusted_return_not_improved": "成本后方向收益没有改善",
    "drawdown_regressed": "窗口回撤恶化过多",
    "holdout_avg_excess_not_improved": "holdout 超额收益没有改善",
    "holdout_avg_net_not_improved": "holdout 成本后收益没有改善",
}

_ROLLBACK_GATE_STATUS_LABELS = {
    "blocked": "未就绪",
    "hold": "继续持有",
    "watchlist": "进入观察",
    "rollback_candidate": "进入 rollback 讨论",
}

_OUT_OF_SAMPLE_STATUS_LABELS = {
    "blocked": "阻断",
    "stable": "稳定",
    "watchlist": "进入观察",
}

_COHORT_STATUS_LABELS = {
    "blocked": "阻断",
    "stable": "稳定",
    "watchlist": "进入观察",
}

_CROSS_SECTIONAL_STATUS_LABELS = {
    "blocked": "阻断",
    "stable": "稳定",
    "watchlist": "进入观察",
}

_ROLLBACK_GATE_REASON_LABELS = {
    "overlap_fixture_blocked": "overlap fixture 未通过",
    "no_validated_rows": "没有 validated rows",
    "validated_rows_below_floor": "validated rows 低于 gate 下限",
}

_OUT_OF_SAMPLE_REASON_LABELS = {
    "overlap_fixture_blocked": "overlap fixture 未通过",
    "no_validated_rows": "没有 validated rows",
    "validated_rows_below_floor": "validated rows 低于 gate 下限",
    "development_rows_below_floor": "development rows 不足",
    "holdout_rows_below_floor": "holdout rows 不足",
    "holdout_hit_rate_regressed": "holdout hit rate 明显回落",
    "holdout_avg_excess_regressed": "holdout 平均超额收益回落过大",
    "holdout_avg_net_regressed": "holdout 成本后方向收益回落过大",
    "holdout_avg_excess_negative": "holdout 平均超额收益转负",
    "holdout_avg_net_negative": "holdout 成本后方向收益转负",
}

_CROSS_SECTIONAL_REASON_LABELS = {
    "no_validated_rows": "没有 validated rows",
    "cross_sectional_cohorts_below_floor": "同日 cohort 数低于 gate 下限",
    "cross_sectional_symbols_below_floor": "同日 symbol 数低于 gate 下限",
    "avg_rank_corr_too_low": "平均 rank correlation 偏低",
    "avg_top_bottom_spread_too_low": "高低分组超额收益 spread 偏低",
    "positive_rank_corr_cohorts_too_few": "正 rank correlation cohort 太少",
    "positive_spread_cohorts_too_few": "高分组跑赢低分组的 cohort 太少",
}

_DIRECTION_LABELS = {
    "positive": "正向",
    "negative": "偏弱",
    "neutral": "中性",
}


def _label(mapping: Mapping[str, str], value: Any, default: str = "—") -> str:
    text = str(value or "").strip()
    if not text:
        return default
    return str(mapping.get(text, text))


def _joined_labels(values: Sequence[Any], mapping: Mapping[str, str], default: str = "—", limit: int = 4) -> str:
    labels = [_label(mapping, item, default="") for item in list(values or []) if str(item or "").strip()]
    labels = [label for label in labels if label]
    if not labels:
        return default
    return " / ".join(labels[:limit])


def _summary_block_lines(rows: Sequence[Sequence[Any]], *, heading: str = "## 执行摘要", lead: str = "") -> List[str]:
    compact_rows = [[str(cell) for cell in row] for row in rows if len(row) >= 2 and any(str(cell).strip() for cell in row)]
    if not compact_rows:
        return []
    lines: List[str] = [heading, ""]
    if lead:
        lines.extend(_section_lead_lines(lead))
    lines.extend(_table(["项目", "结论"], compact_rows))
    return lines


def _action_card_lines(*, heading: str = "## 动作卡片", title: str, summary: str, reason: str, action: str) -> List[str]:
    lines: List[str] = [heading, ""]
    lines.append(f"> **{title}**")
    if summary:
        lines.append(">")
        lines.append(f"> {summary}")
    if reason:
        lines.append(">")
        lines.append(f"> 关键原因：{reason}")
    if action:
        lines.append(">")
        lines.append(f"> 当前动作：{action}")
    return lines


def _row_lookup(rows: Sequence[Sequence[Any]]) -> Mapping[str, str]:
    lookup: dict[str, str] = {}
    for row in rows:
        if len(row) < 2:
            continue
        lookup[str(row[0]).strip()] = str(row[1]).strip()
    return lookup


def _strategy_definition_rows(report_kind: str, payload: Mapping[str, Any]) -> List[List[str]]:
    base_rows: List[List[str]] = [
        ["是不是具体策略", "是，但当前是窄版研究策略：固定 universe、固定目标、固定因子、固定 horizon。不是自动交易系统，也不是今天买卖建议。"],
        ["策略对象", "A 股高流动性普通股票。"],
        ["策略目标", "预测未来 20 个交易日相对中证800的超额收益方向与强弱。"],
        ["核心做法", "把动量 / 相对强弱 / 技术确认 / 流动性 / 风险压成 seed score，再映射成正向 / 中性 / 偏弱判断。"],
    ]
    if report_kind == "validation":
        base_rows.append(
            ["这份报告在回答什么", "它在看这套固定打分逻辑历史上是否稳定，而不是在告诉你今天该不该买这只票。"]
        )
        return base_rows
    variant_rows = [str(row.get("variant", "")).strip() for row in list(payload.get("variant_rows") or []) if str(row.get("variant", "")).strip()]
    variant_text = " / ".join(f"`{item}`" for item in variant_rows[:4]) if variant_rows else "预定义权重变体"
    base_rows.extend(
        [
            ["这份报告在回答什么", "它在比较 baseline 和几种预定义权重变体谁更值得继续验证，不是在宣布已经换策略。"],
            ["当前比较范围", variant_text],
        ]
    )
    return base_rows


def _validation_reader_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    exec_rows = _row_lookup(_validation_exec_summary_rows(payload))
    rollback_status = str(dict(payload.get("rollback_gate") or {}).get("status", "")).strip()
    out_of_sample_status = str(dict(payload.get("out_of_sample_validation") or {}).get("status", "")).strip()
    cross_sectional_status = str(dict(payload.get("cross_sectional_validation") or {}).get("status", "")).strip()
    if rollback_status == "rollback_candidate":
        usable = "现在不能把它当成可用策略，结论已经接近失效或需要 rollback。"
    elif rollback_status == "watchlist" or out_of_sample_status == "watchlist" or cross_sectional_status in {"watchlist", "blocked"}:
        usable = "现在不能把它当成稳定可用策略，只能当观察中的研究结果。"
    else:
        usable = "可以继续跟踪，但还不能把这 1 份 validate 报告当成策略已经定型。"
    return [
        ["一句话结论", exec_rows.get("当前判断", "当前 validate 已产出第一版后验结论。")],
        ["现在能不能用", usable],
        ["你真正该看什么", exec_rows.get("最主要问题", "当前没有额外结构化阻断。")],
        ["它说明了什么", exec_rows.get("这意味着什么", "当前这批样本还不能直接证明策略稳定。")],
        ["下一步", exec_rows.get("下一步", "继续扩大样本并滚动验证。")],
    ]


def _experiment_reader_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    exec_rows = _row_lookup(_experiment_exec_summary_rows(payload))
    promotion_status = str(dict(payload.get("promotion_gate") or {}).get("status", "")).strip()
    if promotion_status == "queue_for_next_stage":
        switchability = "现在还不能直接切换，只能进入下一阶段验证。"
    elif promotion_status == "stay_on_baseline":
        switchability = "现在不能切换，应继续保留 baseline。"
    else:
        switchability = "现在还没资格谈切换，先把样本量和验证门槛补够。"
    return [
        ["一句话结论", exec_rows.get("当前判断", "当前 experiment 还没满足 promotion gate 的最低数据合同。")],
        ["现在能不能切换", switchability],
        ["你真正该看什么", exec_rows.get("为什么还不能直接切换", "当前没有额外阻断。")],
        ["它说明了什么", "这份 experiment 在比较 baseline 和几种预定义权重变体谁更值得继续验证，不是在宣布已经换策略。"],
        ["下一步", exec_rows.get("下一步", "继续扩大样本并做更长窗口复核。")],
    ]


def _prediction_exec_summary_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    status = str(payload.get("status", "")).strip()
    prediction_value = dict(payload.get("prediction_value") or {})
    lag_fixture = dict(payload.get("lag_visibility_fixture") or {})
    benchmark_fixture = dict(payload.get("benchmark_fixture") or {})
    overlap_fixture = dict(payload.get("overlap_fixture") or {})
    supportive = [
        str(factor.get("label", factor.get("factor", ""))).strip()
        for factor in list(payload.get("key_factors") or [])
        if str(factor.get("direction", "")).strip() == "supportive"
    ]
    drags = [
        str(factor.get("label", factor.get("factor", ""))).strip()
        for factor in list(payload.get("key_factors") or [])
        if str(factor.get("direction", "")).strip() != "supportive"
    ]
    if status == "no_prediction":
        current_judgment = "当前被 strategy v1 主合同拒绝，不形成主预测。"
        use_case = "先把它当成合同失败样本，不要包装成今天的强候选。"
        main_boundary = " / ".join(str(item).strip() for item in list(payload.get("no_prediction_reasons") or [])[:2]) or "主合同未通过。"
        next_action = "先解决 universe / 数据可见性 / 流动性门槛，再决定要不要重新预测。"
    else:
        current_judgment = (
            f"{prediction_value.get('summary', '当前已记录 20 日相对收益预测。')} "
            f"当前落在 `{prediction_value.get('expected_rank_bucket', '—')}`，"
            f"置信 `{payload.get('confidence_label', '—')}`，seed score `{float(payload.get('seed_score', 0.0)):.2f}`。"
        )
        direction = str(prediction_value.get("expected_excess_direction", "")).strip()
        if direction == "positive":
            use_case = "更适合作为继续跟踪的正向候选，但还不等于直接交易动作。"
        elif direction == "negative":
            use_case = "更适合作为偏弱或回避对照样本，不要包装成强机会。"
        else:
            use_case = "更适合作为中性观察样本，先看后续确认，不要强行给单边结论。"
        if str(benchmark_fixture.get("status", "")) != "aligned":
            main_boundary = str(benchmark_fixture.get("summary", "")).strip() or "benchmark fixture 还不够完整。"
        elif str(lag_fixture.get("status", "")) in {"partial", "blocked"}:
            main_boundary = str(lag_fixture.get("summary", "")).strip() or "lag / visibility fixture 仍不完整。"
        elif str(overlap_fixture.get("status", "")) == "blocked":
            main_boundary = str(overlap_fixture.get("summary", "")).strip() or "overlap fixture 未通过。"
        else:
            main_boundary = "当前主合同成立，但未来 20 个交易日窗口还没走完，仍需后验验证。"
        blocker_rows = list(dict(payload.get("factor_contract") or {}).get("point_in_time_blockers") or [])
        if blocker_rows:
            blocker = dict(blocker_rows[0] or {})
            next_action = (
                f"先持续跟踪 `{blocker.get('factor_id', '—')}` 这类 point-in-time blocker 是否补齐，"
                "然后等未来窗口结束后跑 validate。"
            )
        else:
            next_action = "先把这条记录留作预测样本，等未来 20 个交易日窗口结束后跑 validate。"
    factor_line = " / ".join(f"`{item}`" for item in supportive[:2]) if supportive else "当前没有明显支撑因子。"
    drag_line = " / ".join(f"`{item}`" for item in drags[:2]) if drags else "当前没有明显拖累因子。"
    return [
        ["当前判断", current_judgment],
        ["现在更适合怎么用", use_case],
        ["主要支撑 / 拖累", f"支撑 {factor_line}；拖累 {drag_line}"],
        ["当前边界", main_boundary],
        ["下一步", next_action],
    ]


def _replay_exec_summary_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    rows = list(payload.get("rows") or [])
    if not rows:
        return [["当前判断", "当前区间没有生成可用 replay 样本。"]]
    predicted_count = sum(1 for row in rows if str(row.get("status", "")).strip() == "predicted")
    no_prediction_count = sum(1 for row in rows if str(row.get("status", "")).strip() == "no_prediction")
    symbol_count = int(payload.get("symbol_count", 1) or 1)
    scores = [float(row.get("seed_score", 0.0)) for row in rows if row.get("seed_score") is not None]
    score_range = f"`{min(scores):.2f}` -> `{max(scores):.2f}`" if scores else "—"
    current_judgment = (
        f"这批回放先给 strategy 建历史样本，共 `{len(rows)}` 条，"
        f"其中 predicted `{predicted_count}` 条，no_prediction `{no_prediction_count}` 条。"
    )
    if symbol_count > 1:
        use_case = "这批样本已经能供给同日 cohort，可开始为 cross-sectional validate 积累底稿，但仍不等于已证明横截面 alpha。"
        supply_summary = dict(payload.get("cross_sectional_supply_summary") or {})
        sample_readiness = str(supply_summary.get("summary", "")).strip() or "当前同日 cohort 覆盖还有限。"
    else:
        use_case = "这批样本只回答单标的时间序列问题，适合先看方向和分桶校准，不回答横截面排序。"
        sample_readiness = "单标的 replay 暂时还不能给 cross-sectional validate 供样。"
    boundary = str(dict(payload.get("overlap_fixture_summary") or {}).get("summary", "")).strip()
    if not boundary:
        notes = [str(note).strip() for note in list(payload.get("notes") or []) if str(note).strip()]
        boundary = notes[0] if notes else "这一步主要负责造样本，不直接证明策略稳定。"
    return [
        ["当前判断", current_judgment],
        ["这批样本适合回答什么", use_case],
        ["样本覆盖", f"`{payload.get('start', '—')}` -> `{payload.get('end', '—')}` | score 区间 {score_range} | asset gap `{payload.get('asset_gap_days', '—')}`"],
        ["validate / xsec 准备度", sample_readiness],
        ["下一步", "如果要看后验表现，就先把 replay 样本入账再跑 validate；如果要比权重，就继续跑 experiment。"],
        ["当前边界", boundary],
    ]


def _validation_exec_summary_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    out_of_sample = dict(payload.get("out_of_sample_validation") or {})
    cross_sectional = dict(payload.get("cross_sectional_validation") or {})
    chronological = dict(payload.get("chronological_cohort_validation") or {})
    rollback_gate = dict(payload.get("rollback_gate") or {})
    rollback_status = str(rollback_gate.get("status", "")).strip()
    if rollback_status == "rollback_candidate":
        current_judgment = "当前这批结果已经接近 rollback 候选，不适合再把它当稳定 baseline。"
    elif rollback_status == "watchlist" or str(out_of_sample.get("status", "")) == "watchlist" or str(cross_sectional.get("status", "")) == "watchlist":
        current_judgment = "当前这批结果已经进入观察，不能只看整体命中率，要重点看最近 holdout 和结构性 miss。"
    elif str(out_of_sample.get("status", "")) == "stable" and str(cross_sectional.get("status", "")) == "stable":
        current_judgment = "当前这批结果暂时稳定，但仍要继续滚动追加样本，不能过早宣称策略定型。"
    else:
        current_judgment = "当前 validate 已产出第一版后验结论，但稳定性证据还不完整。"
    reasons = _joined_labels(
        [*list(out_of_sample.get("decision_reasons") or []), *list(cross_sectional.get("decision_reasons") or []), *list(rollback_gate.get("blockers") or [])],
        {**_OUT_OF_SAMPLE_REASON_LABELS, **_CROSS_SECTIONAL_REASON_LABELS, **_ROLLBACK_GATE_REASON_LABELS},
        default="当前没有额外结构化阻断。",
    )
    next_action = (
        str(cross_sectional.get("next_action", "")).strip()
        or str(out_of_sample.get("next_action", "")).strip()
        or str(rollback_gate.get("next_action", "")).strip()
        or "继续补 non-overlap validated rows，再看最近一段窗口是否稳定。"
    )
    stability = (
        f"out-of-sample `{_label(_OUT_OF_SAMPLE_STATUS_LABELS, out_of_sample.get('status'))}` | "
        f"chronological `{_label(_COHORT_STATUS_LABELS, chronological.get('status'))}` | "
        f"cross-sectional `{_label(_CROSS_SECTIONAL_STATUS_LABELS, cross_sectional.get('status'))}` | "
        f"rollback gate `{_label(_ROLLBACK_GATE_STATUS_LABELS, rollback_status)}`"
    )
    implication = (
        "方向命中不算太差，但成本后收益偏弱，更像“能猜中一部分方向，但还没形成好策略”。"
        if float(payload.get("avg_cost_adjusted_directional_return", 0.0) or 0.0) <= 0
        else "当前不只是命中率，连成本后收益也还在正区间，说明样本至少不是纯噪音。"
    )
    return [
        ["当前判断", current_judgment],
        ["兑现情况", f"hit rate `{float(payload.get('hit_rate', 0.0)):.1%}` | avg excess `{_pct(payload.get('avg_excess_return'))}` | avg net `{_pct(payload.get('avg_cost_adjusted_directional_return'))}` | avg drawdown `{_pct(payload.get('avg_max_drawdown'))}`"],
        ["这意味着什么", implication],
        ["稳定性", stability],
        ["最主要问题", reasons],
        ["下一步", next_action],
    ]


def _validation_headline_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    exec_rows = _row_lookup(_validation_exec_summary_rows(payload))
    out_of_sample = dict(payload.get("out_of_sample_validation") or {})
    cross_sectional = dict(payload.get("cross_sectional_validation") or {})
    rollback_gate = dict(payload.get("rollback_gate") or {})
    rollback_status = str(rollback_gate.get("status", "")).strip()
    out_of_sample_status = str(out_of_sample.get("status", "")).strip()
    cross_sectional_status = str(cross_sectional.get("status", "")).strip()
    if rollback_status == "rollback_candidate":
        current_status = "接近失效"
        usability = "不可用，进入 rollback 讨论"
    elif rollback_status == "watchlist" or out_of_sample_status == "watchlist" or cross_sectional_status in {"watchlist", "blocked"}:
        current_status = "继续观察"
        usability = "暂不可用，不要当成稳定策略"
    elif out_of_sample_status == "stable" and cross_sectional_status == "stable":
        current_status = "可继续跟踪"
        usability = "可继续跟踪，但还没到定型"
    else:
        current_status = "证据不足"
        usability = "暂不下定论，先补样本"
    return [
        ["当前状态", current_status],
        ["可用性", usability],
        ["一句话", exec_rows.get("当前判断", "当前 validate 已产出第一版后验结论。")],
        ["最关键原因", exec_rows.get("最主要问题", "当前没有额外结构化阻断。")],
        ["最实际动作", exec_rows.get("下一步", "继续扩大样本并滚动验证。")],
    ]


def _validation_action_card_lines(payload: Mapping[str, Any]) -> List[str]:
    headline_rows = _row_lookup(_validation_headline_rows(payload))
    title = f"{headline_rows.get('当前状态', '继续观察')}｜{headline_rows.get('可用性', '先补样本')}"
    return _action_card_lines(
        title=title,
        summary=headline_rows.get("一句话", ""),
        reason=headline_rows.get("最关键原因", ""),
        action=headline_rows.get("最实际动作", ""),
    )


def _attribute_exec_summary_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    label_rows = list(payload.get("label_rows") or [])
    dominant = dict(label_rows[0] or {}) if label_rows else {}
    if dominant:
        current_judgment = (
            f"当前最主要的失误类型是 `{dominant.get('label', '—')}`，"
            f"占比 `{float(dominant.get('share', 0.0)):.1%}`。"
        )
        dominant_detail = (
            f"count `{dominant.get('count', 0)}` | hit rate `{float(dominant.get('hit_rate', 0.0)):.1%}` | "
            f"avg excess `{_pct(dominant.get('avg_excess_return'))}` | avg net `{_pct(dominant.get('avg_net_directional_return'))}`"
        )
    else:
        current_judgment = "当前还没有形成足够的归因样本，先别急着改规则。"
        dominant_detail = "当前没有主导归因标签。"
    recommendation = str(list(payload.get("recommendations") or ["继续扩大样本，再看归因是否收敛。"])[0]).strip()
    boundary = " / ".join(str(note).strip() for note in list(payload.get("notes") or [])[:2]) or "当前 attribution 仍是 v1 窄标签集。"
    return [
        ["当前判断", current_judgment],
        ["主导归因", dominant_detail],
        ["现在更该改什么", recommendation],
        ["当前边界", boundary],
    ]


def _experiment_exec_summary_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    promotion_gate = dict(payload.get("promotion_gate") or {})
    rollback_gate = dict(payload.get("rollback_gate") or {})
    status = str(promotion_gate.get("status", "")).strip()
    if status == "queue_for_next_stage":
        current_judgment = "当前最佳 challenger 已通过窄版 promotion gate，可进入下一阶段验证，但还不能直接切换主方案。"
    elif status == "stay_on_baseline":
        current_judgment = "当前最优结论仍是保留 baseline，challenger 还没有稳定跑赢。"
    else:
        current_judgment = "当前 experiment 还没满足 promotion gate 的最低数据合同，先别急着谈切换。"
    reasons = _joined_labels(
        [*list(promotion_gate.get("blockers") or []), *list(promotion_gate.get("decision_reasons") or [])],
        _PROMOTION_GATE_REASON_LABELS,
        default="当前没有额外阻断。",
        limit=6,
    )
    comparison = (
        f"baseline `{promotion_gate.get('baseline_variant', payload.get('baseline_variant', '—')) or '—'}` | "
        f"champion `{payload.get('champion_variant', '—') or '—'}` | "
        f"challenger `{promotion_gate.get('candidate_variant', payload.get('challenger_variant', '—')) or '—'}`"
    )
    evidence = (
        f"primary score delta `{float(promotion_gate.get('primary_score_delta', 0.0)):+.2f}` | "
        f"hit rate delta `{float(promotion_gate.get('hit_rate_delta', 0.0)):+.1%}` | "
        f"avg excess delta `{_pct(promotion_gate.get('avg_excess_return_delta'))}` | "
        f"avg net delta `{_pct(promotion_gate.get('avg_cost_adjusted_directional_return_delta'))}`"
    )
    baseline_health = (
        f"`{_label(_ROLLBACK_GATE_STATUS_LABELS, rollback_gate.get('status'))}` | "
        f"validated `{rollback_gate.get('validated_rows', 0)}/{rollback_gate.get('required_validated_rows', 0)}` | "
        f"hit rate `{_ratio(rollback_gate.get('hit_rate'))}` | "
        f"avg net `{_pct(rollback_gate.get('avg_cost_adjusted_directional_return'))}`"
    )
    next_action = str(promotion_gate.get("next_action", "")).strip() or "继续扩大样本并做更长窗口复核。"
    return [
        ["当前判断", current_judgment],
        ["baseline / champion / challenger", comparison],
        ["为什么还不能直接切换", reasons],
        ["当前增量证据", evidence],
        ["baseline 健康度", baseline_health],
        ["下一步", next_action],
    ]


def _experiment_headline_rows(payload: Mapping[str, Any]) -> List[List[str]]:
    exec_rows = _row_lookup(_experiment_exec_summary_rows(payload))
    promotion_gate = dict(payload.get("promotion_gate") or {})
    status = str(promotion_gate.get("status", "")).strip()
    baseline_variant = str(payload.get("baseline_variant", promotion_gate.get("baseline_variant", "baseline")) or "baseline")
    if status == "queue_for_next_stage":
        current_status = "进入下一阶段验证"
        switch_advice = "不能直接切换，只能继续验证"
    elif status == "stay_on_baseline":
        current_status = f"继续保留 {baseline_variant}"
        switch_advice = "暂不切换，继续保留 baseline"
    else:
        current_status = "先别谈切换"
        switch_advice = "不可切换，先补样本和验证门槛"
    return [
        ["当前状态", current_status],
        ["切换建议", switch_advice],
        ["一句话", exec_rows.get("当前判断", "当前 experiment 还没满足 promotion gate 的最低数据合同。")],
        ["最关键原因", exec_rows.get("为什么还不能直接切换", "当前没有额外阻断。")],
        ["最实际动作", exec_rows.get("下一步", "继续扩大样本并做更长窗口复核。")],
    ]


def _experiment_action_card_lines(payload: Mapping[str, Any]) -> List[str]:
    headline_rows = _row_lookup(_experiment_headline_rows(payload))
    title = f"{headline_rows.get('当前状态', '先别谈切换')}｜{headline_rows.get('切换建议', '先补样本')}"
    return _action_card_lines(
        title=title,
        summary=headline_rows.get("一句话", ""),
        reason=headline_rows.get("最关键原因", ""),
        action=headline_rows.get("最实际动作", ""),
    )


def _benchmark_fixture_lines(fixture: Mapping[str, Any], *, aggregate: bool = False) -> List[str]:
    fixture_block = dict(fixture or {})
    if not fixture_block:
        return []
    lines: List[str] = ["## Benchmark Fixture"]
    summary = str(fixture_block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    if aggregate:
        status_counts = dict(fixture_block.get("status_counts") or {})
        if status_counts:
            lines.append("- 状态分布: " + " / ".join(f"`{key}` `{value}`" for key, value in status_counts.items()))
        lines.append(
            "- 样本: "
            f"`{fixture_block.get('sample_count', 0)}` | "
            f"最小重叠 `{fixture_block.get('min_overlap_rows', 0)}` 行 | "
            f"最大 as_of 偏差 `{fixture_block.get('max_as_of_gap_days', 0)}` 天"
        )
        if "future_window_ready_count" in fixture_block:
            lines.append(
                "- 预测时窗口快照: "
                f"ready `{fixture_block.get('future_window_ready_count', 0)}` | "
                f"pending `{fixture_block.get('future_window_pending_count', 0)}`"
            )
        return lines

    asset_window = dict(fixture_block.get("asset_window") or {})
    benchmark_window = dict(fixture_block.get("benchmark_window") or {})
    blockers = [
        _BENCHMARK_FIXTURE_BLOCKERS.get(str(item), str(item))
        for item in list(fixture_block.get("blockers") or [])
        if str(item).strip()
    ]
    lines.append(
        "- 状态: "
        f"`{fixture_block.get('status', 'unknown')}` | "
        f"overlap `{fixture_block.get('overlap_rows', 0)}` 行 | "
        f"as_of 对齐 `{'是' if fixture_block.get('aligned_as_of') else '否'}` | "
        f"future window `{'ready' if fixture_block.get('future_window_ready') else 'pending'}`"
    )
    lines.append(
        "- 资产窗口: "
        f"`{asset_window.get('start', '—')}` -> `{asset_window.get('end', '—')}` "
        f"(`{asset_window.get('rows', 0)}` 行)"
    )
    lines.append(
        "- 基准窗口: "
        f"`{benchmark_window.get('start', '—')}` -> `{benchmark_window.get('end', '—')}` "
        f"(`{benchmark_window.get('rows', 0)}` 行)"
    )
    lines.append(
        "- as_of: "
        f"资产 `{fixture_block.get('asset_as_of', '—')}` | "
        f"基准 `{fixture_block.get('benchmark_as_of', '—')}` | "
        f"偏差 `{fixture_block.get('as_of_gap_days', 0)}` 天"
    )
    if blockers:
        lines.append("- blockers: " + " / ".join(f"`{item}`" for item in blockers))
    return lines


def _lag_visibility_fixture_lines(fixture: Mapping[str, Any], *, aggregate: bool = False) -> List[str]:
    fixture_block = dict(fixture or {})
    if not fixture_block:
        return []
    lines: List[str] = ["## Lag / Visibility Fixture"]
    summary = str(fixture_block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    if aggregate:
        status_counts = dict(fixture_block.get("status_counts") or {})
        if status_counts:
            lines.append("- 状态分布: " + " / ".join(f"`{key}` `{value}`" for key, value in status_counts.items()))
        lines.append(
            "- 样本: "
            f"`{fixture_block.get('sample_count', 0)}` | "
            f"最少 ready strategy candidate `{fixture_block.get('min_strategy_candidate_ready_count', 0)}` | "
            f"最多 point-in-time blocker `{fixture_block.get('max_point_in_time_blocked_count', 0)}` | "
            f"最大 lag `{fixture_block.get('max_lag_days', 0)}` 天"
        )
        return lines

    lines.append(
        "- 状态: "
        f"`{_LAG_VISIBILITY_STATUS_LABELS.get(str(fixture_block.get('status', '')), str(fixture_block.get('status', 'unknown')))}` | "
        f"strategy candidate ready `{fixture_block.get('strategy_candidate_ready_count', 0)}/{fixture_block.get('strategy_candidate_total', 0)}` | "
        f"point-in-time blocker `{fixture_block.get('point_in_time_blocked_count', 0)}` | "
        f"最大 lag `{fixture_block.get('max_lag_days', 0)}` 天"
    )
    lines.append(
        "- fixture: "
        f"lag ready `{fixture_block.get('lag_ready_count', 0)}` | "
        f"visibility ready `{fixture_block.get('visibility_ready_count', 0)}` | "
        f"degraded `{fixture_block.get('degraded_count', 0)}`"
    )
    blocker_factor_ids = [str(item) for item in list(fixture_block.get("blocker_factor_ids") or []) if str(item).strip()]
    if blocker_factor_ids:
        lines.append("- blockers: " + " / ".join(f"`{item}`" for item in blocker_factor_ids[:6]))
    return lines


def _overlap_fixture_lines(fixture: Mapping[str, Any], *, aggregate: bool = False) -> List[str]:
    fixture_block = dict(fixture or {})
    if not fixture_block:
        return []
    lines: List[str] = ["## Overlap Fixture"]
    summary = str(fixture_block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    if aggregate:
        status_counts = dict(fixture_block.get("status_counts") or {})
        if status_counts:
            lines.append("- 状态分布: " + " / ".join(f"`{key}` `{value}`" for key, value in status_counts.items()))
        lines.append(
            "- 样本: "
            f"`{fixture_block.get('sample_count', 0)}` | "
            f"已比较 `{fixture_block.get('compared_rows', 0)}` | "
            f"重叠 `{fixture_block.get('violation_count', 0)}` | "
            f"最小 gap `{fixture_block.get('min_gap_trading_days', 0)}` 个交易日 | "
            f"最大 required gap `{fixture_block.get('max_required_gap_days', 0)}` 个交易日"
        )
        return lines

    lines.append(
        "- 状态: "
        f"`{_OVERLAP_STATUS_LABELS.get(str(fixture_block.get('status', '')), str(fixture_block.get('status', 'unknown')))}` | "
        f"窗口 `{fixture_block.get('window_start', '—')}` -> `{fixture_block.get('window_end', '—')}` | "
        f"required gap `{fixture_block.get('required_gap_days', 0)}` 个交易日"
    )
    lines.append(
        "- 比较: "
        f"上一主样本 `{fixture_block.get('previous_sample_as_of', '—')}` | "
        f"实际 gap `{fixture_block.get('gap_trading_days', 0)}` 个交易日 | "
        f"policy `{fixture_block.get('overlap_policy', '—')}`"
    )
    return lines


def _promotion_gate_lines(gate: Mapping[str, Any]) -> List[str]:
    gate_block = dict(gate or {})
    if not gate_block:
        return []
    lines: List[str] = ["## Promotion Gate"]
    summary = str(gate_block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    lines.append(
        "- 状态: "
        f"`{_PROMOTION_GATE_STATUS_LABELS.get(str(gate_block.get('status', '')), str(gate_block.get('status', 'unknown')))}` | "
        f"baseline `{gate_block.get('baseline_variant', '—') or '—'}` | "
        f"candidate `{gate_block.get('candidate_variant', '—') or '—'}` | "
        f"可直接切换 `{'是' if gate_block.get('production_ready') else '否'}`"
    )
    lines.append(
        "- 样本: "
        f"总样本 `{gate_block.get('sample_count', 0)}` | "
        f"baseline validated `{gate_block.get('baseline_validated_rows', 0)}` | "
        f"candidate validated `{gate_block.get('candidate_validated_rows', 0)}` | "
        f"gate floor `{gate_block.get('required_validated_rows', 0)}`"
    )
    lines.append(
        "- out-of-sample: "
        f"baseline `{_OUT_OF_SAMPLE_STATUS_LABELS.get(str(gate_block.get('baseline_out_of_sample_status', '')), str(gate_block.get('baseline_out_of_sample_status', '—') or '—'))}` | "
        f"candidate `{_OUT_OF_SAMPLE_STATUS_LABELS.get(str(gate_block.get('candidate_out_of_sample_status', '')), str(gate_block.get('candidate_out_of_sample_status', '—') or '—'))}` | "
        f"holdout excess delta `{_pct(gate_block.get('holdout_avg_excess_return_delta'))}` | "
        f"holdout net delta `{_pct(gate_block.get('holdout_avg_cost_adjusted_directional_return_delta'))}`"
    )
    if gate_block.get("baseline_cross_sectional_status") or gate_block.get("candidate_cross_sectional_status"):
        lines.append(
            "- cross-sectional: "
            f"baseline `{_CROSS_SECTIONAL_STATUS_LABELS.get(str(gate_block.get('baseline_cross_sectional_status', '')), str(gate_block.get('baseline_cross_sectional_status', '—') or '—'))}` | "
            f"candidate `{_CROSS_SECTIONAL_STATUS_LABELS.get(str(gate_block.get('candidate_cross_sectional_status', '')), str(gate_block.get('candidate_cross_sectional_status', '—') or '—'))}`"
        )
    lines.append(
        "- 增量: "
        f"primary score `{float(gate_block.get('primary_score_delta', 0.0)):+.2f}` | "
        f"hit rate `{_pct(gate_block.get('hit_rate_delta'))}` | "
        f"avg excess `{_pct(gate_block.get('avg_excess_return_delta'))}` | "
        f"avg net `{_pct(gate_block.get('avg_cost_adjusted_directional_return_delta'))}` | "
        f"avg drawdown `{_pct(gate_block.get('avg_max_drawdown_delta'))}`"
    )
    blockers = [
        _PROMOTION_GATE_REASON_LABELS.get(str(item), str(item))
        for item in list(gate_block.get("blockers") or [])
        if str(item).strip()
    ]
    reasons = [
        _PROMOTION_GATE_REASON_LABELS.get(str(item), str(item))
        for item in list(gate_block.get("decision_reasons") or [])
        if str(item).strip()
    ]
    if blockers:
        lines.append("- blockers: " + " / ".join(f"`{item}`" for item in blockers))
    if reasons:
        lines.append("- reasons: " + " / ".join(f"`{item}`" for item in reasons))
    if gate_block.get("next_action"):
        lines.append(f"- 下一步: {gate_block.get('next_action')}")
    return lines


def _rollback_gate_lines(gate: Mapping[str, Any]) -> List[str]:
    gate_block = dict(gate or {})
    if not gate_block:
        return []
    lines: List[str] = ["## Rollback Gate"]
    summary = str(gate_block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    lines.append(
        "- 状态: "
        f"`{_ROLLBACK_GATE_STATUS_LABELS.get(str(gate_block.get('status', '')), str(gate_block.get('status', 'unknown')))}` | "
        f"标签 `{gate_block.get('current_label', '—')}` | "
        f"validated `{gate_block.get('validated_rows', 0)}/{gate_block.get('required_validated_rows', 0)}` | "
        f"overlap `{gate_block.get('overlap_violation_count', 0)}`"
    )
    lines.append(
        "- 结果: "
        f"hit rate `{_ratio(gate_block.get('hit_rate'))}` | "
        f"avg excess `{_pct(gate_block.get('avg_excess_return'))}` | "
        f"avg net `{_pct(gate_block.get('avg_cost_adjusted_directional_return'))}` | "
        f"structural miss share `{_ratio(gate_block.get('structural_miss_share'))}`"
    )
    lines.append(
        "- 归因: "
        f"confirmed `{gate_block.get('confirmed_edge_count', 0)}` | "
        f"structural miss `{gate_block.get('structural_miss_count', 0)}` | "
        f"degraded/proxy `{gate_block.get('degraded_miss_count', 0)}` | "
        f"cost drag `{gate_block.get('execution_cost_drag_count', 0)}` | "
        f"horizon mismatch `{gate_block.get('horizon_mismatch_count', 0)}`"
    )
    blockers = [
        _ROLLBACK_GATE_REASON_LABELS.get(str(item), str(item))
        for item in list(gate_block.get("blockers") or [])
        if str(item).strip()
    ]
    if blockers:
        lines.append("- blockers: " + " / ".join(f"`{item}`" for item in blockers))
    if gate_block.get("next_action"):
        lines.append(f"- 下一步: {gate_block.get('next_action')}")
    return lines


def _out_of_sample_validation_lines(payload: Mapping[str, Any]) -> List[str]:
    block = dict(payload or {})
    if not block:
        return []
    lines: List[str] = ["## Out-Of-Sample Validation"]
    summary = str(block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    development = dict(block.get("development_metrics") or {})
    holdout = dict(block.get("holdout_metrics") or {})
    lines.append(
        "- 状态: "
        f"`{_OUT_OF_SAMPLE_STATUS_LABELS.get(str(block.get('status', '')), str(block.get('status', 'unknown')))}` | "
        f"validated `{block.get('validated_rows', 0)}/{block.get('required_validated_rows', 0)}` | "
        f"development `{development.get('count', 0)}` | "
        f"holdout `{holdout.get('count', 0)}`"
    )
    lines.append(
        "- development: "
        f"`{development.get('start_as_of', '—')}` -> `{development.get('end_as_of', '—')}` | "
        f"hit `{_ratio(development.get('hit_rate'))}` | "
        f"avg excess `{_pct(development.get('avg_excess_return'))}` | "
        f"avg net `{_pct(development.get('avg_cost_adjusted_directional_return'))}`"
    )
    lines.append(
        "- holdout: "
        f"`{holdout.get('start_as_of', '—')}` -> `{holdout.get('end_as_of', '—')}` | "
        f"hit `{_ratio(holdout.get('hit_rate'))}` | "
        f"avg excess `{_pct(holdout.get('avg_excess_return'))}` | "
        f"avg net `{_pct(holdout.get('avg_cost_adjusted_directional_return'))}`"
    )
    lines.append(
        "- deltas: "
        f"hit `{_pct(block.get('hit_rate_delta'))}` | "
        f"avg excess `{_pct(block.get('avg_excess_return_delta'))}` | "
        f"avg net `{_pct(block.get('avg_cost_adjusted_directional_return_delta'))}` | "
        f"avg drawdown `{_pct(block.get('avg_max_drawdown_delta'))}`"
    )
    blockers = [
        _OUT_OF_SAMPLE_REASON_LABELS.get(str(item), str(item))
        for item in list(block.get("blockers") or [])
        if str(item).strip()
    ]
    reasons = [
        _OUT_OF_SAMPLE_REASON_LABELS.get(str(item), str(item))
        for item in list(block.get("decision_reasons") or [])
        if str(item).strip()
    ]
    if blockers:
        lines.append("- blockers: " + " / ".join(f"`{item}`" for item in blockers))
    if reasons:
        lines.append("- reasons: " + " / ".join(f"`{item}`" for item in reasons))
    if block.get("next_action"):
        lines.append(f"- 下一步: {block.get('next_action')}")
    return lines


def _chronological_cohort_lines(payload: Mapping[str, Any]) -> List[str]:
    block = dict(payload or {})
    if not block:
        return []
    lines: List[str] = ["## Chronological Cohorts"]
    summary = str(block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    lines.append(f"- 状态: `{_COHORT_STATUS_LABELS.get(str(block.get('status', '')), str(block.get('status', 'unknown')))}`")
    blockers = [
        _OUT_OF_SAMPLE_REASON_LABELS.get(str(item), str(item))
        for item in list(block.get("blockers") or [])
        if str(item).strip()
    ]
    if blockers:
        lines.append("- blockers: " + " / ".join(f"`{item}`" for item in blockers))
        return lines
    cohort_rows = list(block.get("cohort_rows") or [])
    if cohort_rows:
        lines.append("| cohort | as_of range | count | hit rate | avg excess | avg net |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in cohort_rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("label", "—")),
                        f"{row.get('start_as_of', '—')} -> {row.get('end_as_of', '—')}",
                        str(row.get("count", 0)),
                        _ratio(row.get("hit_rate")),
                        _pct(row.get("avg_excess_return")),
                        _pct(row.get("avg_cost_adjusted_directional_return")),
                    ]
                )
                + " |"
            )
    lines.append(
        "- latest vs earliest: "
        f"hit `{_pct(block.get('hit_rate_delta_latest_vs_earliest'))}` | "
        f"avg excess `{_pct(block.get('avg_excess_return_delta_latest_vs_earliest'))}` | "
        f"avg net `{_pct(block.get('avg_cost_adjusted_directional_return_delta_latest_vs_earliest'))}`"
    )
    return lines


def _cross_sectional_validation_lines(payload: Mapping[str, Any]) -> List[str]:
    block = dict(payload or {})
    if not block:
        return []
    lines: List[str] = ["## Cross-Sectional Validation"]
    summary = str(block.get("summary", "")).strip()
    if summary:
        lines.extend(["", f"- {summary}"])
    lines.append(
        "- 状态: "
        f"`{_CROSS_SECTIONAL_STATUS_LABELS.get(str(block.get('status', '')), str(block.get('status', 'unknown')))}` | "
        f"cohort `{block.get('cohort_count', 0)}/{block.get('required_cohorts', 0)}` | "
        f"eligible symbols `{block.get('eligible_symbol_count', 0)}` | "
        f"每个 cohort 最少 symbols `{block.get('required_cohort_symbols', 0)}`"
    )
    lines.append(
        "- 总结: "
        f"avg rank corr `{float(block.get('avg_rank_corr', 0.0)):+.2f}` | "
        f"avg top-bottom excess `{_pct(block.get('avg_top_bottom_spread'))}` | "
        f"avg top-bottom net `{_pct(block.get('avg_top_bottom_net_spread'))}` | "
        f"positive rank corr cohort `{block.get('positive_rank_corr_count', 0)}` | "
        f"positive spread cohort `{block.get('positive_spread_count', 0)}`"
    )
    blockers = [
        _CROSS_SECTIONAL_REASON_LABELS.get(str(item), str(item))
        for item in list(block.get("blockers") or [])
        if str(item).strip()
    ]
    reasons = [
        _CROSS_SECTIONAL_REASON_LABELS.get(str(item), str(item))
        for item in list(block.get("decision_reasons") or [])
        if str(item).strip()
    ]
    if blockers:
        lines.append("- blockers: " + " / ".join(f"`{item}`" for item in blockers))
    if reasons:
        lines.append("- reasons: " + " / ".join(f"`{item}`" for item in reasons))
    cohort_rows = list(block.get("eligible_cohort_rows") or [])
    if cohort_rows:
        lines.append("| as_of | symbols | rank corr | top-bottom excess | top-bottom net |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in cohort_rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("as_of", "—")),
                        str(row.get("symbol_count", 0)),
                        f"{float(row.get('rank_corr', 0.0)):+.2f}",
                        _pct(row.get("top_bottom_spread")),
                        _pct(row.get("top_bottom_net_spread")),
                    ]
                )
                + " |"
            )
    if block.get("next_action"):
        lines.append(f"- 下一步: {block.get('next_action')}")
    return lines


def _replay_symbol_coverage_lines(payload: Mapping[str, Any]) -> List[str]:
    symbol_rows = list(payload.get("symbol_rows") or [])
    if not symbol_rows:
        return []
    lines: List[str] = ["## Symbol Coverage"]
    lines.append("| symbol | status | samples | predicted | no_prediction | as_of range |")
    lines.append("| --- | --- | --- | --- | --- | --- |")
    for row in symbol_rows:
        as_of_range = f"{row.get('first_as_of', '—')} -> {row.get('last_as_of', '—')}"
        lines.append(
            "| "
            + " | ".join(
                [
                    f"`{row.get('symbol', '')}`",
                    str(row.get("status", "—")),
                    str(row.get("sample_count", 0)),
                    str(row.get("predicted_count", 0)),
                    str(row.get("no_prediction_count", 0)),
                    as_of_range,
                ]
            )
            + " |"
        )
        if row.get("error"):
            lines.append(f"  失败原因: {row.get('error')}")
    return lines


def _replay_cross_sectional_supply_lines(payload: Mapping[str, Any]) -> List[str]:
    summary = dict(payload.get("cross_sectional_supply_summary") or {})
    if not summary:
        return []
    lines: List[str] = ["## Same-Day Cohorts"]
    if summary.get("summary"):
        lines.extend(["", f"- {summary.get('summary')}"])
    lines.append(
        "- 覆盖: "
        f"cohort `{summary.get('cohort_count', 0)}` | "
        f"unique symbols `{summary.get('unique_symbol_count', 0)}` | "
        f"`>=2` symbols `{summary.get('cohorts_ge_2', 0)}` | "
        f"`>=3` symbols `{summary.get('cohorts_ge_3', 0)}`"
    )
    lines.append(
        "- 范围: "
        f"每个日期最少 `{summary.get('min_symbols_per_as_of', 0)}` 只 | "
        f"最多 `{summary.get('max_symbols_per_as_of', 0)}` 只"
    )
    cohort_rows = list(summary.get("cohort_rows") or [])
    if cohort_rows:
        lines.append("| as_of | symbols | predicted | no_prediction |")
        lines.append("| --- | --- | --- | --- |")
        for row in cohort_rows:
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("as_of", "—")),
                        str(row.get("symbol_count", 0)),
                        str(row.get("predicted_count", 0)),
                        str(row.get("no_prediction_count", 0)),
                    ]
                )
                + " |"
            )
    return lines


def _batch_source_lines(payload: Mapping[str, Any]) -> List[str]:
    batch_context = dict(payload.get("batch_context") or {})
    if not batch_context:
        return []
    lines: List[str] = ["## Batch Source"]
    if batch_context.get("summary"):
        lines.extend(["", f"- {batch_context.get('summary')}"])
    lines.append(
        "- 来源: "
        f"`{batch_context.get('label', batch_context.get('key', '—'))}`"
        f" (`{batch_context.get('key', '—') or '—'}`) | "
        f"mode `{batch_context.get('mode', '—')}` | "
        f"source symbols `{batch_context.get('source_symbol_count', 0)}` | "
        f"effective symbols `{batch_context.get('effective_symbol_count', batch_context.get('source_symbol_count', 0))}`"
    )
    if int(batch_context.get("watchlist_match_count", 0) or 0) > 0 or int(batch_context.get("explicit_symbol_count", 0) or 0) > 0:
        lines.append(
            "- 组成: "
            f"显式 `{batch_context.get('explicit_symbol_count', 0)}` | "
            f"watchlist 命中 `{batch_context.get('watchlist_match_count', 0)}`"
        )
    for note in list(batch_context.get("notes") or []):
        lines.append(f"- 备注: {note}")
    return lines


def _cohort_recipe_lines(payload: Mapping[str, Any]) -> List[str]:
    cohort_recipe = dict(payload.get("cohort_recipe") or {})
    if not cohort_recipe:
        return []
    lines: List[str] = ["## Cohort Recipe"]
    if cohort_recipe.get("summary"):
        lines.extend(["", f"- {cohort_recipe.get('summary')}"])
    lines.append(
        "- 配置: "
        f"`{cohort_recipe.get('label', cohort_recipe.get('key', '—'))}`"
        f" (`{cohort_recipe.get('key', '—') or '—'}`) | "
        f"asset gap `{cohort_recipe.get('asset_gap_days', '—')}` | "
        f"max samples `{cohort_recipe.get('max_samples', '—')}`"
    )
    if cohort_recipe.get("applied_via"):
        lines.append(
            "- 应用方式: "
            f"`{cohort_recipe.get('applied_via', '—')}` | "
            f"配置默认 gap `{cohort_recipe.get('configured_asset_gap_days', cohort_recipe.get('asset_gap_days', '—'))}` | "
            f"配置默认 max `{cohort_recipe.get('configured_max_samples', cohort_recipe.get('max_samples', '—'))}`"
        )
    for note in list(cohort_recipe.get("notes") or []):
        lines.append(f"- 备注: {note}")
    return lines


class StrategyReportRenderer:
    """Render strategy prediction ledger views."""

    def render_prediction(self, payload: Mapping[str, Any], *, persisted: bool) -> str:
        lines: List[str] = ["# Strategy Prediction Ledger", ""]
        status = "已写入账本" if persisted else "仅预览，未写入账本"
        lines.append(f"- 状态: `{payload.get('status', 'unknown')}` | {status}")
        lines.append(f"- ID: `{payload.get('prediction_id', '')}`")
        lines.append(f"- 标的: `{payload.get('name', payload.get('symbol', ''))}` (`{payload.get('symbol', '')}`)")
        lines.append(
            f"- 合同: `{payload.get('universe', '')}` | `{payload.get('prediction_target', '')}` | `{dict(payload.get('horizon') or {}).get('label', '')}`"
        )
        lines.append(
            f"- 时点: `as_of {payload.get('as_of', '—')}` | `effective_from {payload.get('effective_from', '—') or '—'}` | `visibility {payload.get('visibility_class', '')}`"
        )
        lines.append("")
        lines.append("## 一句话结论")
        if str(payload.get("status", "")) == "no_prediction":
            lines.append("- 当前按 strategy v1 合同拒绝给主预测，原因在于 universe / 数据可见性 / 流动性门槛没有全部满足。")
        else:
            prediction_value = dict(payload.get("prediction_value") or {})
            lines.append(
                f"- {prediction_value.get('summary', '当前已记录 20 日相对收益预测。')} 当前置信桶为 `{payload.get('confidence_label', '—')}`，"
                f"seed score `{float(payload.get('seed_score', 0.0)):.2f}`。"
            )
        exec_summary_lines = _summary_block_lines(
            _prediction_exec_summary_rows(payload),
            lead="这段只回答当前更像强候选、弱候选还是合同失败样本，以及现在更适合怎么处理。",
        )
        if exec_summary_lines:
            lines.extend(["", *exec_summary_lines])
        lines.append("")
        lines.append("## 预测合同")
        lines.append(f"- benchmark: `{dict(payload.get('benchmark') or {}).get('name', '')}` (`{dict(payload.get('benchmark') or {}).get('symbol', '')}`)")
        lines.append(f"- confidence_type: `{payload.get('confidence_type', '')}`")
        lines.append(
            f"- cohort: 每 `{dict(payload.get('cohort_contract') or {}).get('cohort_frequency_days', '—')}` 个交易日一组，"
            f"主持有期 `{dict(payload.get('cohort_contract') or {}).get('holding_period_days', '—')}` 个交易日。"
        )
        benchmark_fixture_lines = _benchmark_fixture_lines(payload.get("benchmark_fixture") or {})
        if benchmark_fixture_lines:
            lines.extend(["", *benchmark_fixture_lines])
        lag_visibility_fixture_lines = _lag_visibility_fixture_lines(payload.get("lag_visibility_fixture") or {})
        if lag_visibility_fixture_lines:
            lines.extend(["", *lag_visibility_fixture_lines])
        overlap_fixture_lines = _overlap_fixture_lines(payload.get("overlap_fixture") or {})
        if overlap_fixture_lines:
            lines.extend(["", *overlap_fixture_lines])
        lines.append("")
        lines.append("## 关键因子")
        for factor in list(payload.get("key_factors") or []):
            lines.append(
                f"- `{factor.get('label', factor.get('factor', ''))}`: score `{float(factor.get('score', 0.0)):.1f}`，"
                f"{'支撑' if factor.get('direction') == 'supportive' else '拖累'}因素。{factor.get('summary', '')}"
            )
        if not list(payload.get("key_factors") or []):
            lines.append("- 当前没有留下可复核的关键因子。")
        lines.append("")
        lines.append("## 因子快照")
        snapshot = dict(payload.get("factor_snapshot") or {})
        factor_contract = dict(payload.get("factor_contract") or {})
        momentum = dict(snapshot.get("price_momentum") or {})
        relative = dict(snapshot.get("benchmark_relative") or {})
        technical = dict(snapshot.get("technical") or {})
        liquidity = dict(snapshot.get("liquidity") or {})
        risk = dict(snapshot.get("risk") or {})
        lines.append(
            f"- 动量: 5日 `{_pct(momentum.get('return_5d'))}` | 20日 `{_pct(momentum.get('return_20d'))}` | 60日 `{_pct(momentum.get('return_60d'))}`"
        )
        if relative:
            lines.append(
                f"- 相对基准: 20日超额 `{_pct(relative.get('relative_return_20d'))}` | 60日超额 `{_pct(relative.get('relative_return_60d'))}`"
            )
        lines.append(
            f"- 技术: MA `{technical.get('ma_signal', '—')}` | MACD `{technical.get('macd_signal', '—')}` | RSI `{technical.get('rsi', '—')}`"
        )
        lines.append(
            f"- 流动性: 20日日均成交额 `{float(liquidity.get('avg_turnover_20d', 0.0)) / 1e8:.2f}` 亿 | 60日中位成交额 `{float(liquidity.get('median_turnover_60d', 0.0)) / 1e8:.2f}` 亿"
        )
        lines.append(
            f"- 风险: 20日波动 `{_pct(risk.get('volatility_20d'))}` | 1年最大回撤 `{_pct(risk.get('max_drawdown_1y'))}`"
        )
        if factor_contract:
            families = dict(factor_contract.get("families") or {})
            states = dict(factor_contract.get("states") or {})
            blockers = list(factor_contract.get("point_in_time_blockers") or [])
            lines.append("")
            lines.append("## 因子合同")
            if families:
                lines.append("- family 覆盖: " + " / ".join(f"`{key}` `{value}`" for key, value in families.items()))
            if states:
                lines.append("- 状态分布: " + " / ".join(f"`{key}` `{value}`" for key, value in states.items()))
            candidate_ids = list(factor_contract.get("strategy_candidate_factor_ids") or [])
            if candidate_ids:
                lines.append("- strategy 候选: " + " / ".join(f"`{item}`" for item in candidate_ids[:8]))
            else:
                lines.append("- strategy 候选: 当前没有满足 point-in-time 合同的强因子进入 challenger。")
            if blockers:
                blocker = dict(blockers[0] or {})
                lines.append(
                    f"- point-in-time blocker: `{blocker.get('factor_id', '—')}` | {blocker.get('reason', 'lag / visibility fixture incomplete')}"
                )
        lines.append("")
        lines.append("## 证据时点与来源")
        evidence = dict(payload.get("evidence_sources") or {})
        lines.append(
            f"- 行情: `as_of {evidence.get('market_data_as_of', '—')}`，来源 `{evidence.get('market_data_source', '—')}`。"
        )
        lines.append(
            f"- 基准: `as_of {evidence.get('benchmark_as_of', '—')}`，来源 `{evidence.get('benchmark_source', '—')}`。"
        )
        lines.append(
            f"- 催化: `as_of {evidence.get('catalyst_evidence_as_of', '—')}`，来源 `{ ' / '.join(list(evidence.get('catalyst_sources') or [])) or '未命中高置信直连源' }`。"
        )
        if evidence.get("point_in_time_note"):
            lines.append(f"- point-in-time: {evidence.get('point_in_time_note')}")
        for note in list(evidence.get("notes") or []):
            lines.append(f"- 边界: {note}")
        lines.append("")
        lines.append("## 降级与边界")
        flags = list(payload.get("downgrade_flags") or [])
        if flags:
            for flag in flags:
                lines.append(f"- `{flag}`")
        else:
            lines.append("- 当前没有额外降级标记。")
        if str(payload.get("status", "")) == "no_prediction":
            lines.append("")
            lines.append("## 拒绝预测原因")
            for reason in list(payload.get("no_prediction_reasons") or []):
                lines.append(f"- {reason}")
        if list(payload.get("notes") or []):
            lines.append("")
            lines.append("## 备注")
            for note in list(payload.get("notes") or []):
                lines.append(f"- {note}")
        validation = dict(payload.get("validation") or {})
        if validation.get("validation_status") == "validated":
            lines.append("")
            lines.append("## 后验验证")
            lines.append(
                f"- 区间: `{validation.get('window_start', '—')}` -> `{validation.get('window_end', '—')}` | "
                f"超额收益 `{_pct(validation.get('excess_return'))}` | 成本后方向收益 `{_pct(validation.get('cost_adjusted_directional_return'))}`"
            )
            lines.append(
                f"- 结果: `{'命中' if validation.get('hit') else '未命中'}` | "
                f"最大回撤 `{_pct(validation.get('max_drawdown'))}` | 验证方向 `{validation.get('direction_checked', '—')}`"
            )
        attribution = dict(payload.get("attribution") or {})
        if attribution:
            lines.append("")
            lines.append("## 归因")
            lines.append(
                f"- 标签: `{attribution.get('label', '—')}` | 状态 `{attribution.get('status', '—')}` | {attribution.get('summary', '')}"
            )
            if attribution.get("next_action"):
                lines.append(f"- 下一步: {attribution.get('next_action')}")
        return "\n".join(lines).rstrip() + "\n"

    def render_prediction_list(self, rows: Sequence[Mapping[str, Any]]) -> str:
        lines: List[str] = ["# Strategy Prediction Ledger", ""]
        if not rows:
            lines.append("- 当前账本为空。先运行 `python -m src.commands.strategy predict 600519` 之类的命令写入首条预测。")
            return "\n".join(lines) + "\n"

        lines.append("| as_of | symbol | status | rank bucket | confidence | score |")
        lines.append("| --- | --- | --- | --- | --- | --- |")
        for row in rows:
            prediction_value = dict(row.get("prediction_value") or {})
            validation = dict(row.get("validation") or {})
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("as_of", "—")),
                        f"`{row.get('symbol', '')}`",
                        f"`{row.get('status', '')}`",
                        str(prediction_value.get("expected_rank_bucket", "—")),
                        str(row.get("confidence_label", "—")),
                        f"{float(row.get('seed_score', 0.0)):.2f}",
                    ]
                )
                + " |"
            )
        return "\n".join(lines).rstrip() + "\n"

    def render_replay_summary(self, payload: Mapping[str, Any], *, persisted: bool) -> str:
        rows = list(payload.get("rows") or [])
        symbol_count = int(payload.get("symbol_count", 1) or 1)
        symbols = [str(item) for item in list(payload.get("symbols") or []) if str(item).strip()]
        lines: List[str] = ["# Strategy Replay", ""]
        if symbol_count > 1:
            lines.append(
                f"- 标的数: `{symbol_count}` | 区间: `{payload.get('start', '—')}` -> `{payload.get('end', '—')}` | "
                f"样本数: `{len(rows)}` | {'已写入账本' if persisted else '仅预览'}"
            )
            lines.append(f"- 标的: " + " / ".join(f"`{symbol}`" for symbol in symbols))
            lines.append(
                f"- 合同: `multi-symbol replay supply v1` | 资产重入间隔 `{payload.get('asset_gap_days', '—')}` 个交易日 | 主 horizon `20个交易日`"
            )
        else:
            lines.append(
                f"- 标的: `{payload.get('symbol', '')}` | 区间: `{payload.get('start', '—')}` -> `{payload.get('end', '—')}` | "
                f"样本数: `{len(rows)}` | {'已写入账本' if persisted else '仅预览'}"
            )
            lines.append(
                f"- 合同: `single-symbol historical replay` | 资产重入间隔 `{payload.get('asset_gap_days', '—')}` 个交易日 | 主 horizon `20个交易日`"
            )
        exec_summary_lines = _summary_block_lines(
            _replay_exec_summary_rows(payload),
            lead="这段只回答这批 replay 样本现在能说明什么、还不能说明什么，以及下一步该接 validate 还是 experiment。",
        )
        if exec_summary_lines:
            lines.extend(["", *exec_summary_lines])
        for note in list(payload.get("notes") or []):
            lines.append(f"- 说明: {note}")
        batch_source_lines = _batch_source_lines(payload)
        if batch_source_lines:
            lines.extend(["", *batch_source_lines])
        cohort_recipe_lines = _cohort_recipe_lines(payload)
        if cohort_recipe_lines:
            lines.extend(["", *cohort_recipe_lines])
        benchmark_fixture_lines = _benchmark_fixture_lines(payload.get("benchmark_fixture_summary") or {}, aggregate=True)
        if benchmark_fixture_lines:
            lines.extend(["", *benchmark_fixture_lines])
        lag_visibility_fixture_lines = _lag_visibility_fixture_lines(payload.get("lag_visibility_fixture_summary") or {}, aggregate=True)
        if lag_visibility_fixture_lines:
            lines.extend(["", *lag_visibility_fixture_lines])
        overlap_fixture_lines = _overlap_fixture_lines(payload.get("overlap_fixture_summary") or {}, aggregate=True)
        if overlap_fixture_lines:
            lines.extend(["", *overlap_fixture_lines])
        if symbol_count > 1:
            symbol_coverage_lines = _replay_symbol_coverage_lines(payload)
            if symbol_coverage_lines:
                lines.extend(["", *symbol_coverage_lines])
            supply_lines = _replay_cross_sectional_supply_lines(payload)
            if supply_lines:
                lines.extend(["", *supply_lines])
        lines.append("")
        if not rows:
            lines.append("- 当前区间内没有生成可用 replay 样本。")
            return "\n".join(lines).rstrip() + "\n"
        if symbol_count > 1:
            lines.append("| as_of | symbol | status | rank bucket | confidence | score |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
        else:
            lines.append("| as_of | status | rank bucket | confidence | score |")
            lines.append("| --- | --- | --- | --- | --- |")
        for row in rows:
            prediction_value = dict(row.get("prediction_value") or {})
            columns = [
                str(row.get("as_of", "—")),
                str(row.get("status", "—")),
                str(prediction_value.get("expected_rank_bucket", "—")),
                str(row.get("confidence_label", "—")),
                f"{float(row.get('seed_score', 0.0)):.2f}",
            ]
            if symbol_count > 1:
                columns.insert(1, f"`{row.get('symbol', '')}`")
            lines.append("| " + " | ".join(columns) + " |")
        return "\n".join(lines).rstrip() + "\n"

    def render_validation_summary(self, payload: Mapping[str, Any], *, persisted: bool, client_facing: bool = False) -> str:
        lines: List[str] = ["# Strategy Validation", ""]
        mode_label = "正式成稿" if client_facing else ("已回写账本" if persisted else "仅预览")
        lines.append(
            f"- 样本总数: `{payload.get('total_rows', 0)}` | 已验证: `{payload.get('validated_rows', 0)}` | "
            f"待未来窗口: `{payload.get('pending_rows', 0)}` | {mode_label}"
        )
        lines.append(
            f"- 有预测: `{payload.get('predicted_rows', 0)}` | 无预测: `{payload.get('no_prediction_rows', 0)}` | "
            f"跳过: `{payload.get('skipped_rows', 0)}`"
        )
        action_card_lines = _validation_action_card_lines(payload)
        if action_card_lines:
            lines.extend(["", *action_card_lines])
        headline_lines = _summary_block_lines(
            _validation_headline_rows(payload),
            heading="## 当前结论",
            lead="如果你只看 10 秒，先看这张表：它直接告诉你当前能不能用。",
        )
        if headline_lines:
            lines.extend(["", *headline_lines])
        strategy_definition_lines = _summary_block_lines(
            _strategy_definition_rows("validation", payload),
            heading="## 这套策略是什么",
            lead="这段先说明 strategy 在看什么，不是今天买卖建议。",
        )
        if strategy_definition_lines:
            lines.extend(["", *strategy_definition_lines])
        reader_lines = _summary_block_lines(
            _validation_reader_rows(payload),
            heading="## 这次到底看出来什么",
            lead="这段直接回答你看完后到底该怎么理解。",
        )
        if reader_lines:
            lines.extend(["", *reader_lines])
        exec_summary_lines = _summary_block_lines(
            _validation_exec_summary_rows(payload),
            lead="这段只回答这批样本当前算稳定、观察还是接近失效，以及最该盯哪一个风险。",
        )
        if exec_summary_lines:
            lines.extend(["", *exec_summary_lines])
        lines.append("")
        lines.append("## 总体结果")
        lines.append(f"- hit rate: `{float(payload.get('hit_rate', 0.0)):.1%}`")
        lines.append(f"- 平均超额收益: `{_pct(payload.get('avg_excess_return'))}`")
        lines.append(f"- 平均成本后方向收益: `{_pct(payload.get('avg_cost_adjusted_directional_return'))}`")
        lines.append(f"- 平均窗口最大回撤: `{_pct(payload.get('avg_max_drawdown'))}`")
        benchmark_fixture_lines = _benchmark_fixture_lines(payload.get("benchmark_fixture_summary") or {}, aggregate=True)
        if benchmark_fixture_lines:
            lines.extend(["", *benchmark_fixture_lines])
        lag_visibility_fixture_lines = _lag_visibility_fixture_lines(payload.get("lag_visibility_fixture_summary") or {}, aggregate=True)
        if lag_visibility_fixture_lines:
            lines.extend(["", *lag_visibility_fixture_lines])
        overlap_fixture_lines = _overlap_fixture_lines(payload.get("overlap_fixture_summary") or {}, aggregate=True)
        if overlap_fixture_lines:
            lines.extend(["", *overlap_fixture_lines])
        out_of_sample_lines = _out_of_sample_validation_lines(payload.get("out_of_sample_validation") or {})
        if out_of_sample_lines:
            lines.extend(["", *out_of_sample_lines])
        cohort_lines = _chronological_cohort_lines(payload.get("chronological_cohort_validation") or {})
        if cohort_lines:
            lines.extend(["", *cohort_lines])
        cross_sectional_lines = _cross_sectional_validation_lines(payload.get("cross_sectional_validation") or {})
        if cross_sectional_lines:
            lines.extend(["", *cross_sectional_lines])
        rollback_gate_lines = _rollback_gate_lines(payload.get("rollback_gate") or {})
        if rollback_gate_lines:
            lines.extend(["", *rollback_gate_lines])
        bucket_rows = list(payload.get("bucket_rows") or [])
        if bucket_rows:
            lines.append("")
            lines.append("## 置信度分桶")
            lines.append("| bucket | count | hit rate | avg excess | avg net directional |")
            lines.append("| --- | --- | --- | --- | --- |")
            for row in bucket_rows:
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("bucket", "—")),
                            str(row.get("count", 0)),
                            f"{float(row.get('hit_rate', 0.0)):.1%}",
                            _pct(row.get("avg_excess_return")),
                            _pct(row.get("avg_net_directional_return")),
                        ]
                    )
                    + " |"
                )
        recent_rows = list(payload.get("recent_rows") or [])
        if recent_rows:
            lines.append("")
            lines.append("## 最近样本")
            lines.append("| as_of | symbol | direction | confidence | excess | net directional | hit | status |")
            lines.append("| --- | --- | --- | --- | --- | --- | --- | --- |")
            for row in recent_rows:
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("as_of", "—")),
                            f"`{row.get('symbol', '')}`",
                            _label(_DIRECTION_LABELS, row.get("direction"), str(row.get("direction", "—"))),
                            str(row.get("confidence_label", "—")),
                            _pct(row.get("excess_return")),
                            _pct(row.get("net_directional_return")),
                            "✅" if bool(row.get("hit")) else "❌",
                            str(row.get("validation_status", "—")),
                        ]
                    )
                    + " |"
                )
        notes = list(payload.get("notes") or [])
        if notes:
            lines.append("")
            lines.append("## 边界")
            for note in notes:
                lines.append(f"- {note}")
        return "\n".join(lines).rstrip() + "\n"

    def render_attribute_summary(self, payload: Mapping[str, Any], *, persisted: bool) -> str:
        lines: List[str] = ["# Strategy Attribution", ""]
        lines.append(
            f"- 样本总数: `{payload.get('total_rows', 0)}` | 已归因: `{payload.get('attributed_rows', 0)}` | "
            f"待未来窗口: `{payload.get('pending_rows', 0)}` | 不适用: `{payload.get('not_applicable_rows', 0)}` | "
            f"{'已回写账本' if persisted else '仅预览'}"
        )
        exec_summary_lines = _summary_block_lines(
            _attribute_exec_summary_rows(payload),
            lead="这段只回答当前最主要的问题像权重失衡、缺因子还是周期错配，以及下一轮先改什么。",
        )
        if exec_summary_lines:
            lines.extend(["", *exec_summary_lines])
        label_rows = list(payload.get("label_rows") or [])
        if label_rows:
            lines.append("")
            lines.append("## 归因分桶")
            lines.append("| label | count | share | hit rate | avg excess | avg net directional |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for row in label_rows:
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("label", "—")),
                            str(row.get("count", 0)),
                            f"{float(row.get('share', 0.0)):.1%}",
                            f"{float(row.get('hit_rate', 0.0)):.1%}",
                            _pct(row.get("avg_excess_return")),
                            _pct(row.get("avg_net_directional_return")),
                        ]
                    )
                    + " |"
                )
        recent_rows = list(payload.get("recent_rows") or [])
        if recent_rows:
            lines.append("")
            lines.append("## 最近样本")
            lines.append("| as_of | symbol | label | excess | hit | status |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for row in recent_rows:
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("as_of", "—")),
                            f"`{row.get('symbol', '')}`",
                            str(row.get("label", "—")),
                            _pct(row.get("excess_return")),
                            "✅" if bool(row.get("hit")) else "❌",
                            str(row.get("status", "—")),
                        ]
                    )
                    + " |"
                )
                if row.get("summary"):
                    lines.append(f"  说明: {row.get('summary')}")
                if row.get("next_action"):
                    lines.append(f"  下一步: {row.get('next_action')}")
        recommendations = list(payload.get("recommendations") or [])
        if recommendations:
            lines.append("")
            lines.append("## 下一轮建议")
            for item in recommendations:
                lines.append(f"- {item}")
        notes = list(payload.get("notes") or [])
        if notes:
            lines.append("")
            lines.append("## 边界")
            for note in notes:
                lines.append(f"- {note}")
        return "\n".join(lines).rstrip() + "\n"

    def render_experiment_summary(self, payload: Mapping[str, Any]) -> str:
        symbol_count = int(payload.get("symbol_count", 1) or 1)
        symbols = [str(item) for item in list(payload.get("symbols") or []) if str(item).strip()]
        lines: List[str] = ["# Strategy Experiment", ""]
        if symbol_count > 1:
            lines.append(
                f"- 标的数: `{symbol_count}` | 区间: `{payload.get('start', '—')}` -> `{payload.get('end', '—')}` | "
                f"样本数: `{payload.get('sample_count', 0)}` | baseline: `{payload.get('baseline_variant', '')}`"
            )
            lines.append(f"- 标的: " + " / ".join(f"`{symbol}`" for symbol in symbols))
        else:
            lines.append(
                f"- 标的: `{payload.get('symbol', '')}` | 区间: `{payload.get('start', '—')}` -> `{payload.get('end', '—')}` | "
                f"样本数: `{payload.get('sample_count', 0)}` | baseline: `{payload.get('baseline_variant', '')}`"
            )
        lines.append(
            f"- 当前 champion: `{payload.get('champion_variant', '—') or '—'}` | challenger: `{payload.get('challenger_variant', '—') or '—'}`"
        )
        action_card_lines = _experiment_action_card_lines(payload)
        if action_card_lines:
            lines.extend(["", *action_card_lines])
        headline_lines = _summary_block_lines(
            _experiment_headline_rows(payload),
            heading="## 当前结论",
            lead="如果你只看 10 秒，先看这张表：它直接告诉你现在能不能切换。",
        )
        if headline_lines:
            lines.extend(["", *headline_lines])
        strategy_definition_lines = _summary_block_lines(
            _strategy_definition_rows("experiment", payload),
            heading="## 这套策略是什么",
            lead="这段先说明 strategy 在看什么，不是在宣布已经换策略。",
        )
        if strategy_definition_lines:
            lines.extend(["", *strategy_definition_lines])
        reader_lines = _summary_block_lines(
            _experiment_reader_rows(payload),
            heading="## 这次到底看出来什么",
            lead="这段直接回答你看完后到底该怎么理解。",
        )
        if reader_lines:
            lines.extend(["", *reader_lines])
        exec_summary_lines = _summary_block_lines(
            _experiment_exec_summary_rows(payload),
            lead="这段只回答 baseline 要不要留、challenger 能不能升级，以及为什么现在还不能直接切换主方案。",
        )
        if exec_summary_lines:
            lines.extend(["", *exec_summary_lines])
        batch_source_lines = _batch_source_lines(payload)
        if batch_source_lines:
            lines.extend(["", *batch_source_lines])
        cohort_recipe_lines = _cohort_recipe_lines(payload)
        if cohort_recipe_lines:
            lines.extend(["", *cohort_recipe_lines])
        benchmark_fixture_lines = _benchmark_fixture_lines(payload.get("benchmark_fixture_summary") or {}, aggregate=True)
        if benchmark_fixture_lines:
            lines.extend(["", *benchmark_fixture_lines])
        lag_visibility_fixture_lines = _lag_visibility_fixture_lines(payload.get("lag_visibility_fixture_summary") or {}, aggregate=True)
        if lag_visibility_fixture_lines:
            lines.extend(["", *lag_visibility_fixture_lines])
        overlap_fixture_lines = _overlap_fixture_lines(payload.get("overlap_fixture_summary") or {}, aggregate=True)
        if overlap_fixture_lines:
            lines.extend(["", *overlap_fixture_lines])
        if symbol_count > 1:
            supply_lines = _replay_cross_sectional_supply_lines(payload)
            if supply_lines:
                lines.extend(["", *supply_lines])
        promotion_gate_lines = _promotion_gate_lines(payload.get("promotion_gate") or {})
        if promotion_gate_lines:
            lines.extend(["", *promotion_gate_lines])
        rollback_gate_lines = _rollback_gate_lines(payload.get("rollback_gate") or {})
        if rollback_gate_lines:
            lines.extend(["", *rollback_gate_lines])
        variant_rows = list(payload.get("variant_rows") or [])
        if variant_rows:
            lines.append("")
            lines.append("## 变体对比")
            lines.append("| variant | validated | oos | xsec | hit rate | avg excess | avg net directional | avg drawdown | dominant attribution |")
            lines.append("| --- | --- | --- | --- | --- | --- | --- | --- | --- |")
            for row in variant_rows:
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("variant", "—")),
                            str(row.get("validated_sample_count", row.get("sample_count", 0))),
                            _OUT_OF_SAMPLE_STATUS_LABELS.get(str(row.get("out_of_sample_status", "")), str(row.get("out_of_sample_status", "—"))),
                            _CROSS_SECTIONAL_STATUS_LABELS.get(str(row.get("cross_sectional_status", "")), str(row.get("cross_sectional_status", "—"))),
                            f"{float(row.get('hit_rate', 0.0)):.1%}",
                            _pct(row.get("avg_excess_return")),
                            _pct(row.get("avg_cost_adjusted_directional_return")),
                            _pct(row.get("avg_max_drawdown")),
                            str(row.get("dominant_attribution", "—")),
                        ]
                    )
                    + " |"
                )
                if row.get("hypothesis"):
                    lines.append(f"  假设: {row.get('hypothesis')}")
                if "cross_sectional_avg_rank_corr" in row:
                    lines.append(f"  xsec rank corr: `{float(row.get('cross_sectional_avg_rank_corr', 0.0)):+.2f}`")
                if "holdout_avg_excess_return" in row:
                    lines.append(
                        "  holdout: "
                        f"`{int(row.get('holdout_rows', 0))}` 样本 | "
                        f"avg excess `{_pct(row.get('holdout_avg_excess_return'))}` | "
                        f"avg net `{_pct(row.get('holdout_avg_cost_adjusted_directional_return'))}`"
                    )
        notes = list(payload.get("notes") or [])
        if notes:
            lines.append("")
            lines.append("## 边界")
            for note in notes:
                lines.append(f"- {note}")
        return "\n".join(lines).rstrip() + "\n"
