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


def _append_section(lines: List[str], title: str, items: List[str]) -> None:
    lines.extend(["", f"## {title}"])
    if items:
        for item in items:
            lines.append(f"- {item}")
    else:
        lines.append("- 暂无。")


class BriefingRenderer:
    """Render daily/weekly briefing payloads to markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"- 生成时间: `{payload['generated_at']}`",
        ]

        _append_section(lines, "今日主线", payload.get("headline_lines", []))
        _append_section(lines, "隔夜与主要资产", payload.get("overnight_lines", []))
        _append_section(lines, "宏观与流动性", payload.get("macro_items", []))
        _append_section(lines, "市场概览", payload.get("market_overview_lines", []))
        _append_section(lines, "全球资金流代理", payload.get("flow_lines", []))
        _append_section(lines, "情绪代理", payload.get("sentiment_lines", []))

        lines.extend(["", "## Watchlist 雷达"])
        lines.extend(
            _table(
                ["标的", "最新价", "1日", "5日", "20日", "趋势", "量比"],
                payload.get("watchlist_rows", []),
            )
        )

        _append_section(lines, "重点观察", payload.get("focus_lines", []))
        _append_section(lines, "风格与轮动", payload.get("rotation_lines", []))
        _append_section(lines, "关注提醒", payload.get("alerts", []))
        _append_section(lines, "今日已知事件", payload.get("event_lines", []))
        _append_section(lines, "组合与 Thesis", payload.get("portfolio_lines", []))
        _append_section(lines, "今日跟踪清单", payload.get("calendar_lines", []))
        _append_section(lines, "行动建议", payload.get("action_lines", []))

        return "\n".join(lines)
