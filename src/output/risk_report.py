"""Markdown renderer for portfolio risk output."""

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


class RiskReportRenderer:
    """Render risk payloads into markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"- 生成时间: `{payload['generated_at']}`",
            "",
        ]

        summary_lines = payload.get("summary_lines", [])
        if summary_lines:
            lines.append("## 组合概览")
            for item in summary_lines:
                lines.append(f"- {item}")
            lines.append("")

        metric_lines = payload.get("metric_lines", [])
        if metric_lines:
            lines.append("## 核心指标")
            for item in metric_lines:
                lines.append(f"- {item}")
            lines.append("")

        limit_alerts = payload.get("limit_alerts", [])
        lines.append("## 风险限制")
        if limit_alerts:
            for item in limit_alerts:
                lines.append(f"- {item}")
        else:
            lines.append("- 当前未触发硬阈值告警。")
        lines.append("")

        correlation_rows = payload.get("correlation_rows", [])
        if correlation_rows:
            lines.append("## 相关性矩阵")
            lines.extend(_table(payload.get("correlation_headers", []), correlation_rows))
            lines.append("")

        concentration_alerts = payload.get("concentration_alerts", [])
        lines.append("## 集中度提醒")
        if concentration_alerts:
            for item in concentration_alerts:
                lines.append(f"- {item}")
        else:
            lines.append("- 当前没有超过相关性阈值的持仓对。")
        lines.append("")

        stress_lines = payload.get("stress_lines", [])
        if stress_lines:
            lines.append("## 压力测试")
            for item in stress_lines:
                lines.append(f"- {item}")
            lines.append("")

        stress_rows = payload.get("stress_rows", [])
        if stress_rows:
            lines.extend(_table(["标的", "权重", "冲击", "贡献", "映射"], stress_rows))
            lines.append("")

        coverage_notes = payload.get("coverage_notes", [])
        if coverage_notes:
            lines.append("## 数据覆盖")
            for item in coverage_notes:
                lines.append(f"- {item}")

        return "\n".join(lines).rstrip()
