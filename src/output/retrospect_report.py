"""Markdown renderer for decision retrospective reports."""

from __future__ import annotations

from typing import Any, Dict, List


def _table(headers: List[str], rows: List[List[str]]) -> List[str]:
    lines = [
        "| " + " | ".join(headers) + " |",
        "| " + " | ".join(["---"] * len(headers)) + " |",
    ]
    for row in rows:
        lines.append("| " + " | ".join(row) + " |")
    return lines


def _pct(value: Any) -> str:
    if value is None:
        return "—"
    try:
        return f"{float(value) * 100:+.2f}%"
    except Exception:
        return "—"


class DecisionRetrospectReportRenderer:
    """Render portfolio decision retrospectives into markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        month = str(payload.get("month", ""))
        symbol = str(payload.get("symbol", "")).strip()
        title = "决策回溯" if not symbol else f"决策回溯: {symbol}"
        lines = [
            f"# {title}",
            "",
            f"- 复盘月份: `{month}`",
            f"- 生成时间: `{payload.get('generated_at', '')}`",
            f"- 标准观察窗口: `{payload.get('lookahead', 20)}` 个交易日",
            f"- 标准止损: `-{float(payload.get('stop_pct', 0.08)) * 100:.1f}%`",
            f"- 标准目标: `+{float(payload.get('target_pct', 0.15)) * 100:.1f}%`",
            "",
            "## 这次主要看到了什么",
        ]
        for item in payload.get("summary_lines", []):
            lines.append(f"- {item}")

        basis_rows = payload.get("basis_rows", [])
        if basis_rows:
            lines.extend(["", "## Basis 统计"])
            lines.extend(_table(["Basis", "笔数", "平均结果", "胜率", "止损触发率", "目标触达率"], basis_rows))

        items = payload.get("items", [])
        if not items:
            lines.extend(["", "## 原始决策", "- 该月份没有可回溯的交易记录。"])
            lines.extend(["", "## 为什么当时会做这个决定", "- 当前没有可分析的决策样本。"])
            lines.extend(["", "## 后验路径", "- 当前没有可分析的价格路径。"])
            lines.extend(["", "## 复盘结论", "- 当前没有可复盘的结论。"])
            return "\n".join(lines)

        lines.extend(["", "## 逐笔回溯"])
        for index, item in enumerate(items, start=1):
            thesis = dict(item.get("thesis") or {})
            signal_snapshot = dict(item.get("signal_snapshot") or {})
            lines.extend(
                [
                    "",
                    f"### {index}. {item.get('name', item.get('symbol'))} ({item.get('symbol')})",
                    "",
                    "## 原始决策",
                ]
            )
            decision_rows = [
                ["动作", str(item.get("action", ""))],
                ["Basis", str(item.get("basis", ""))],
                ["决策时间", str(item.get("timestamp", ""))],
                ["入场基准日", str(item.get("entry_date", ""))],
                ["入场价格", f"{float(item.get('entry_price', 0.0)):.3f}"],
                ["备注", str(item.get("note", "") or "—")],
                ["核心 thesis", str(thesis.get("core_assumption", "") or "—")],
                ["验证指标", str(thesis.get("validation_metric", "") or "—")],
                ["止损条件", str(thesis.get("stop_condition", "") or "—")],
                ["预期周期", str(thesis.get("holding_period", "") or "—")],
            ]
            lines.extend(_table(["项目", "内容"], decision_rows))

            lines.extend(["", "## 为什么当时会做这个决定"])
            for reason in item.get("reason_lines", []):
                lines.append(f"- {reason}")
            if not item.get("reason_lines"):
                lines.append("- 当时的结构化理由缺失，说明以后需要把 thesis 和信号快照记得更完整。")

            lines.extend(["", "## 后验路径"])
            path_rows = [
                ["1日", item.get("forward_returns", {}).get("1d", "—")],
                ["3日", item.get("forward_returns", {}).get("3d", "—")],
                ["5日", item.get("forward_returns", {}).get("5d", "—")],
                ["20日", item.get("forward_returns", {}).get("20d", "—")],
                ["最大有利波动(MFE)", _pct(item.get("mfe"))],
                ["最大不利波动(MAE)", _pct(item.get("mae"))],
                ["标准止损位", "—" if item.get("stop_level") is None else f"{float(item['stop_level']):.3f}"],
                ["标准目标位", "—" if item.get("target_level") is None else f"{float(item['target_level']):.3f}"],
                ["最先发生的事件", str(item.get("first_event", "—"))],
                ["样本覆盖", f"{int(item.get('coverage_days', 0))} 个交易日"],
            ]
            lines.extend(_table(["后验指标", "结果"], path_rows))

            signal_rows = [
                ["信号一致性", str(item.get("signal_alignment", "—"))],
                ["MA 信号", str(signal_snapshot.get("ma_signal", "—") or "—")],
                ["MACD 信号", str(signal_snapshot.get("macd_signal", "—") or "—")],
                ["RSI", "—" if signal_snapshot.get("rsi") is None else f"{float(signal_snapshot['rsi']):.1f}"],
                ["ADX", "—" if signal_snapshot.get("adx") is None else f"{float(signal_snapshot['adx']):.1f}"],
                ["量价结构", str(signal_snapshot.get("volume_structure", "—") or "—")],
                ["近20日收益", _pct(signal_snapshot.get("return_20d"))],
                ["价格分位(1y)", _pct(signal_snapshot.get("price_percentile_1y"))],
            ]
            lines.extend(["", "### 当时信号快照"])
            lines.extend(_table(["项目", "内容"], signal_rows))

            verdict = dict(item.get("verdict") or {})
            lines.extend(["", "## 复盘结论"])
            lines.append(f"- 结果判断：`{verdict.get('outcome', '—')}`")
            lines.append(f"- 复盘摘要：{verdict.get('summary', '—')}")
            lines.append(f"- 具体解释：{verdict.get('detail', '—')}")
            if not item.get("thesis_is_historical") and thesis:
                lines.append("- 这笔交易没有历史 thesis 快照，以上 thesis 为当前记录回填，不能完全等同于当时原文。")

        return "\n".join(lines)
