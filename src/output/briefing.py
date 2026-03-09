"""Briefing markdown renderer."""

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


class BriefingRenderer:
    """Render daily/weekly briefing payloads to markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"- 生成时间: `{payload['generated_at']}`",
            "",
            "## 宏观与跨市场快照",
        ]
        for item in payload.get("macro_items", []):
            lines.append(f"- {item}")

        lines.extend(["", "## Watchlist 概览"])
        lines.extend(
            _table(
                ["标的", "最新价", "1日", "5日", "20日", "趋势"],
                payload.get("watchlist_rows", []),
            )
        )

        alerts = payload.get("alerts", [])
        lines.extend(["", "## 关注提醒"])
        if alerts:
            for alert in alerts:
                lines.append(f"- {alert}")
        else:
            lines.append("- 当前没有触发强提醒。")

        portfolio_lines = payload.get("portfolio_lines", [])
        lines.extend(["", "## 组合摘要"])
        if portfolio_lines:
            for line in portfolio_lines:
                lines.append(f"- {line}")
        else:
            lines.append("- 当前没有持仓记录。")

        return "\n".join(lines)
