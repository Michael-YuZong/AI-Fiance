"""Markdown renderers for strategy prediction ledger."""

from __future__ import annotations

from typing import Any, List, Mapping, Sequence


def _pct(value: Any) -> str:
    try:
        number = float(value)
    except Exception:
        return "—"
    return f"{number * 100:+.2f}%"


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
        lines.append("")
        lines.append("## 预测合同")
        lines.append(f"- benchmark: `{dict(payload.get('benchmark') or {}).get('name', '')}` (`{dict(payload.get('benchmark') or {}).get('symbol', '')}`)")
        lines.append(f"- confidence_type: `{payload.get('confidence_type', '')}`")
        lines.append(
            f"- cohort: 每 `{dict(payload.get('cohort_contract') or {}).get('cohort_frequency_days', '—')}` 个交易日一组，"
            f"主持有期 `{dict(payload.get('cohort_contract') or {}).get('holding_period_days', '—')}` 个交易日。"
        )
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
        lines: List[str] = ["# Strategy Replay", ""]
        lines.append(
            f"- 标的: `{payload.get('symbol', '')}` | 区间: `{payload.get('start', '—')}` -> `{payload.get('end', '—')}` | "
            f"样本数: `{len(rows)}` | {'已写入账本' if persisted else '仅预览'}"
        )
        lines.append(
            f"- 合同: `single-symbol historical replay` | 资产重入间隔 `{payload.get('asset_gap_days', '—')}` 个交易日 | 主 horizon `20个交易日`"
        )
        for note in list(payload.get("notes") or []):
            lines.append(f"- 说明: {note}")
        lines.append("")
        if not rows:
            lines.append("- 当前区间内没有生成可用 replay 样本。")
            return "\n".join(lines).rstrip() + "\n"
        lines.append("| as_of | status | rank bucket | confidence | score |")
        lines.append("| --- | --- | --- | --- | --- |")
        for row in rows:
            prediction_value = dict(row.get("prediction_value") or {})
            lines.append(
                "| "
                + " | ".join(
                    [
                        str(row.get("as_of", "—")),
                        str(row.get("status", "—")),
                        str(prediction_value.get("expected_rank_bucket", "—")),
                        str(row.get("confidence_label", "—")),
                        f"{float(row.get('seed_score', 0.0)):.2f}",
                    ]
                )
                + " |"
            )
        return "\n".join(lines).rstrip() + "\n"

    def render_validation_summary(self, payload: Mapping[str, Any], *, persisted: bool) -> str:
        lines: List[str] = ["# Strategy Validation", ""]
        lines.append(
            f"- 样本总数: `{payload.get('total_rows', 0)}` | 已验证: `{payload.get('validated_rows', 0)}` | "
            f"待未来窗口: `{payload.get('pending_rows', 0)}` | {'已回写账本' if persisted else '仅预览'}"
        )
        lines.append(
            f"- predicted: `{payload.get('predicted_rows', 0)}` | no_prediction: `{payload.get('no_prediction_rows', 0)}` | "
            f"skipped: `{payload.get('skipped_rows', 0)}`"
        )
        lines.append("")
        lines.append("## 总体结果")
        lines.append(f"- hit rate: `{float(payload.get('hit_rate', 0.0)):.1%}`")
        lines.append(f"- 平均超额收益: `{_pct(payload.get('avg_excess_return'))}`")
        lines.append(f"- 平均成本后方向收益: `{_pct(payload.get('avg_cost_adjusted_directional_return'))}`")
        lines.append(f"- 平均窗口最大回撤: `{_pct(payload.get('avg_max_drawdown'))}`")
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
                            str(row.get("direction", "—")),
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
        lines: List[str] = ["# Strategy Experiment", ""]
        lines.append(
            f"- 标的: `{payload.get('symbol', '')}` | 区间: `{payload.get('start', '—')}` -> `{payload.get('end', '—')}` | "
            f"样本数: `{payload.get('sample_count', 0)}` | baseline: `{payload.get('baseline_variant', '')}`"
        )
        lines.append(
            f"- 当前 champion: `{payload.get('champion_variant', '—') or '—'}` | challenger: `{payload.get('challenger_variant', '—') or '—'}`"
        )
        variant_rows = list(payload.get("variant_rows") or [])
        if variant_rows:
            lines.append("")
            lines.append("## 变体对比")
            lines.append("| variant | hit rate | avg excess | avg net directional | avg drawdown | dominant attribution |")
            lines.append("| --- | --- | --- | --- | --- | --- |")
            for row in variant_rows:
                lines.append(
                    "| "
                    + " | ".join(
                        [
                            str(row.get("variant", "—")),
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
        notes = list(payload.get("notes") or [])
        if notes:
            lines.append("")
            lines.append("## 边界")
            for note in notes:
                lines.append(f"- {note}")
        return "\n".join(lines).rstrip() + "\n"
