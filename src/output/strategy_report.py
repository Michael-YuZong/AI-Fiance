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
        momentum = dict(snapshot.get("price_momentum") or {})
        technical = dict(snapshot.get("technical") or {})
        liquidity = dict(snapshot.get("liquidity") or {})
        risk = dict(snapshot.get("risk") or {})
        lines.append(
            f"- 动量: 5日 `{_pct(momentum.get('return_5d'))}` | 20日 `{_pct(momentum.get('return_20d'))}` | 60日 `{_pct(momentum.get('return_60d'))}`"
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
