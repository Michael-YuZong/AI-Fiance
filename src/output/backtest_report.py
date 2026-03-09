"""Markdown renderer for backtest output."""

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


class BacktestReportRenderer:
    """Render backtest results into markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"- 标的: `{payload['symbol']}`",
            f"- 规则: `{payload['rule_name']}`",
            f"- 区间: `{payload['period']}`",
            f"- 生成时间: `{payload['generated_at']}`",
            "",
            f"> {payload['description']}",
            "",
        ]

        if payload.get("error"):
            lines.append(f"- 结果: {payload['error']}")
            return "\n".join(lines)

        lines.append("## 策略表现")
        for item in payload.get("metric_lines", []):
            lines.append(f"- {item}")

        baseline_lines = payload.get("baseline_lines", [])
        if baseline_lines:
            lines.extend(["", "## 基准对比"])
            for item in baseline_lines:
                lines.append(f"- {item}")

        warnings = payload.get("warnings", [])
        if warnings:
            lines.extend(["", "## 重要提示"])
            for item in warnings:
                lines.append(f"- {item}")

        trade_rows = payload.get("trade_rows", [])
        if trade_rows:
            lines.extend(["", "## 交易样本"])
            lines.extend(_table(["进场", "离场", "进场价", "离场价", "收益"], trade_rows))

        return "\n".join(lines)
