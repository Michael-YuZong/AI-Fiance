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


def _append_block(lines: List[str], title: str, summary: str = "") -> None:
    lines.extend(["", f"## {title}"])
    if summary:
        lines.extend(["", summary])


def _append_subsection(lines: List[str], title: str, items: List[str]) -> None:
    lines.extend(["", f"### {title}"])
    if not items:
        lines.append("")
        lines.append("暂无。")
        return
    lines.append("")
    lines.append(items[0])
    for item in items[1:]:
        lines.append(f"- {item}")


def _first_item(payload: Dict[str, Any], key: str) -> str:
    items = payload.get(key, []) or []
    return str(items[0]) if items else ""


class BriefingRenderer:
    """Render daily/weekly briefing payloads to markdown."""

    def render(self, payload: Dict[str, Any]) -> str:
        lines = [
            f"# {payload['title']}",
            "",
            f"- 生成时间: `{payload['generated_at']}`",
        ]

        _append_block(lines, "执行摘要", _first_item(payload, "headline_lines"))
        for item in (payload.get("headline_lines", []) or [])[1:]:
            lines.append(f"- {item}")

        _append_block(
            lines,
            "市场驱动",
            _first_item(payload, "narrative_validation_lines") or _first_item(payload, "story_lines"),
        )
        _append_subsection(lines, "主线校验", payload.get("narrative_validation_lines", []))
        _append_subsection(lines, "重要催化", payload.get("important_event_lines", []))
        _append_subsection(lines, "新闻主线", payload.get("news_lines", []))
        _append_subsection(lines, "新闻推演", payload.get("story_lines", []))
        _append_subsection(lines, "宏观与流动性", payload.get("macro_items", []))

        _append_block(
            lines,
            "盘面结构",
            _first_item(payload, "rotation_driver_lines") or _first_item(payload, "market_pulse_lines"),
        )
        _append_subsection(lines, "板块轮动", payload.get("rotation_driver_lines", []))
        _append_subsection(lines, "主力资金流向", payload.get("main_flow_driver_lines", []))
        _append_subsection(lines, "全市场脉搏", payload.get("market_pulse_lines", []))
        _append_subsection(lines, "龙虎榜与涨停池", payload.get("lhb_lines", []))
        _append_subsection(lines, "关键宏观资产", payload.get("monitor_lines", []))
        _append_subsection(lines, "隔夜与主要资产", payload.get("overnight_lines", []))
        _append_subsection(lines, "全球资金流代理", payload.get("flow_lines", []))
        _append_subsection(lines, "情绪代理", payload.get("sentiment_lines", []))

        _append_block(
            lines,
            "观察池与组合",
            _first_item(payload, "market_overview_lines") or _first_item(payload, "focus_lines"),
        )
        _append_subsection(lines, "市场概览", payload.get("market_overview_lines", []))

        lines.extend(["", "### Watchlist 雷达", ""])
        lines.extend(
            _table(
                ["标的", "最新价", "1日", "5日", "20日", "趋势", "量比", "技术"],
                payload.get("watchlist_rows", []),
            )
        )
        _append_subsection(lines, "Watchlist 技术指标", payload.get("watchlist_technical_lines", []))
        _append_subsection(lines, "重点观察", payload.get("focus_lines", []))
        _append_subsection(lines, "风格与轮动", payload.get("rotation_lines", []))
        _append_subsection(lines, "组合与 Thesis", payload.get("portfolio_lines", []))

        _append_block(
            lines,
            "行动计划",
            _first_item(payload, "action_lines") or _first_item(payload, "verification_lines"),
        )
        _append_subsection(lines, "关注提醒", payload.get("alerts", []))
        _append_subsection(lines, "今日已知事件", payload.get("event_lines", []))
        _append_subsection(lines, "今日验证点", payload.get("verification_lines", []))
        _append_subsection(lines, "今日跟踪清单", payload.get("calendar_lines", []))
        _append_subsection(lines, "行动建议", payload.get("action_lines", []))

        return "\n".join(lines)
